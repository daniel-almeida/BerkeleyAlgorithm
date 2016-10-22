/* CPSC 538B - Distributed Systems. Daniel Almeida - Assignment 2 */

/*
Periodically, the master queries all of the slave nodes for their local time and
each node replies with their current time.  The master then computes the time
difference between its local clock and that of each of the slaves (deriving a
set of delta values, one per slave). Next, the master computes avg, a fault
tolerant average of the delta values: an average over the largest set of delta
values that differ from one another by at most a pre-defined constant d. Then,
for each slave the master computes a time correction value by computing the
difference between avg and the delta value for the slave. Finally, the master
sends this correction value to each slave for the slaves to adjust their clocks.
*/

package main

import (
	"fmt"
	"os"
	"bufio"
	"time"
	"strconv"
	"net"
	"strings"
	"sort"
	"github.com/arcaneiceman/GoVector/capture"
	"github.com/arcaneiceman/GoVector/govec"
)

type Slave struct {
    Address string
    Clock int
    Delta int
}

type Slaves []Slave

func (slice Slaves) Len() int {
    return len(slice)
}

func (slice Slaves) Less(i, j int) bool {
    return slice[i].Clock < slice[j].Clock;
}

func (slice Slaves) Swap(i, j int) {
    slice[i], slice[j] = slice[j], slice[i]
}

func (s *Slave) run(logFile string) {

	Logger := govec.Initialize("slave"+s.Address, logFile)
	go startClock(&s.Clock)

	// Listen to host:ip
	// slaveAddr, err := net.ResolveUDPAddr("udp", s.Address)
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	// slaveConn, err := net.ListenUDP("udp", slaveAddr)
	conn, err := net.ListenPacket("udp", s.Address)
	checkError(err)

	defer conn.Close()

	// Create byte array with underlying array to hold up to 1024 bytes
	b := make([]byte, 1024)
	
	lastKnownSyncRound := -1

	for {
		var msgReceived string
		// Receive message from Master
		// n, masterAddress, err := conn.ReadFromUDP(b[:])
		n, masterAddress, err := capture.ReadFrom(conn.ReadFrom, b[0:])
		checkError(err)

		Logger.UnpackReceive("Received message from master: ", b[0:n], &msgReceived)
		// fmt.Println("Received ", string(b[:n]), "from master at ", masterAddress)

		// msgReceived := string(b[:n])
		splitMsg := strings.Split(msgReceived, " ")

		if len(splitMsg) < 2 || len(splitMsg) > 4 {
			fmt.Println("Malformed message from ", masterAddress)
			continue
		}

		syncRound := splitMsg[0]
		syncRoundInt, err := strconv.Atoi(syncRound)
		checkError(err)

		action := splitMsg[1]

		if action == "request" {
			lastKnownSyncRound = syncRoundInt
			t1 := splitMsg[2]
			t2 := s.Clock

			// Send local clock back to Master
			responseMsg := syncRound + " clock " + t1 + " " + strconv.Itoa(t2)
			msgBuf := Logger.PrepareSend("Sending message to master: ", responseMsg)

			// n, err = conn.WriteToUDP([]byte(msgBuf), masterAddress)
			_, err = capture.WriteTo(conn.WriteTo, msgBuf, masterAddress)
			checkError(err)

		} else if action == "update"  && syncRoundInt >= lastKnownSyncRound{
			// Update local clock (sync)
			clockAdjust, err := strconv.Atoi(splitMsg[2])
			checkError(err)
			
			fmt.Printf("Clock %v adjusted (%v) to ", s.Clock, clockAdjust)
			s.Clock = s.Clock + clockAdjust
			fmt.Printf("%v\n", s.Clock)
		}
		
	}

	fmt.Println("Hello world.")	
}

type Master struct {
	Address string
	Clock int
	Threshold int
	SlavesFile string
	LogFile string
	Slaves map[string]*Slave
	// Conn *net.UDPConn
	Conn net.PacketConn
	SyncRound int
	Logger *govec.GoLog
	// slaves []Slave
}

func main() {
	// Check number of arguments
	numArgs := len(os.Args)
	// fmt.Println(numArgs)
	if numArgs < 2 || numArgs > 7 { 
		panic("Not enough arguments.") 
	}

	ipPort := os.Args[2]
	// fmt.Println("ip:port -> ", ipPort)

	initialClockString := os.Args[3]
	initialClock, err := strconv.Atoi(initialClockString)
	checkError(err)
	// fmt.Println("Initial clock: ", initialClock)

	// Parse flag (master or slave)
	masterOrSlaveFlag := os.Args[1]
	if masterOrSlaveFlag == "-m" {
		// Master is running
		if numArgs != 7 {
			panic("Expected 6 arguments.")
		}

		thresholdString := os.Args[4]
		threshold, err := strconv.Atoi(thresholdString)
		checkError(err)

		slavesFile := os.Args[5]
		logFile := os.Args[6]

		master := Master{ipPort, initialClock, threshold, slavesFile, logFile, make(map[string]*Slave), nil, 0, nil}
		master.run()

	} else if masterOrSlaveFlag == "-s" {
		// Slave is running
		if numArgs != 5 {
			panic("Expected 4 arguments.")
		}

		logFile := os.Args[4]
		slave := Slave{ipPort, initialClock, -1}
		slave.run(logFile)
	} else {
		panic("Flag should be -m or -s")
	}
}

func (m *Master) run() {
	m.Logger = govec.Initialize("master", m.LogFile)
	
	go startClock(&m.Clock)
	
	m.loadSlavesFromFile()
	// fmt.Println(m.Slaves)

	// Set up UDPConn
	m.establishConnection()

	// Start listener to hear back from Slaves
	go m.listenToSlaves()

	// var responseChans = [5]chan int
	// for i := range responseChans {
	// 	responseChans[i] := make(chan int)
	// }
	// res := make([]int, len(slaves))
	for {
		for _, v := range m.Slaves {
			v.Clock = -1
			go m.sendClockRequest(v)
		}

		syncRoundTimeout := time.NewTimer(time.Second * 5)
		<-syncRoundTimeout.C

		// Update syncRound. Messages received from now on will be ignored until the next sendClockRequest
		m.SyncRound++

		// Calculate fault-tolerant average
		validSlaves := make(Slaves, 0)
		for _, v := range m.Slaves {
			if v.Clock > -1 {
				validSlaves = append(validSlaves, *v)
			}
		}
		
		avg := calculateAvg(validSlaves, m.Threshold)


		// Send updated clock to each slave
		for _, v := range m.Slaves {
			fmt.Printf("Sending clock adjust. AVG: %v ; Delta: %v; Final value: %v\n", avg, v.Delta, v.Delta-avg)
			go m.sendClockUpdate(v, m.SyncRound, v.Delta-avg)
		}
		m.Clock = m.Clock - avg

		// for _, v := range m.Slaves {
		// 	fmt.Printf("Slave %v - clock: %v, delta %v\n", v.Address, v.Clock, v.Delta)
		// }
	}
}

func calculateAvg(slavesAlive Slaves, d int) int {
	fmt.Println("Valid slaves: ", slavesAlive)
	sort.Sort(slavesAlive)
	fmt.Println("Valid slaves sorted: ", slavesAlive)
	sizeLargestSubset := 0
	avg := 0

	for i, v := range slavesAlive {
		sizeOfSubset := 1
		totalDelta := v.Delta
		for j, v2 := range slavesAlive {
			if i == j {
				continue
			}
			diff := v.Delta - v2.Delta
			if diff < 0 {
				diff = -diff
			}
			if diff <= d {
				sizeOfSubset++
				totalDelta += v2.Delta
			}
		}
		if sizeOfSubset > sizeLargestSubset {
			sizeLargestSubset = sizeOfSubset
			avg = totalDelta/sizeOfSubset
		}
	}
	fmt.Println("Size of subset: ", sizeLargestSubset)
	fmt.Println("Avg delta: ", avg)
	return avg
}

func (m *Master) establishConnection() {
	// masterAddr, err := net.ResolveUDPAddr("udp", m.Address)
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }

	// m.Conn, err = net.ListenUDP("udp", masterAddr)
	var err error

	m.Conn, err = net.ListenPacket("udp", m.Address)
	checkError(err)
}

func (m *Master) listenToSlaves() {
	b := make([]byte, 1024)

	for {
		var messageReceived string
		// n, slaveAddr, err := m.Conn.ReadFromUDP(b[0:])
		n, slaveAddr, err := capture.ReadFrom(m.Conn.ReadFrom, b[0:])
		checkError(err)

        
		m.Logger.UnpackReceive("Received message from slave", b[0:n], &messageReceived)
        t3 := m.Clock

        slaveAddress := slaveAddr.String()
        fmt.Println(messageReceived, " from Slave at ", slaveAddress)
		
		splitMsg := strings.Split(messageReceived, " ")

		if len(splitMsg) != 4 {
			fmt.Println("Malformed message from ", slaveAddress)
			continue
		}


		syncRound, err := strconv.Atoi(splitMsg[0])
		checkError(err)

		if syncRound < m.SyncRound {
			fmt.Println("Message out syncRound.")
			continue
		}

		action := splitMsg[1]

		if action == "clock" {
			t1, err := strconv.Atoi(splitMsg[2])
			checkError(err)

			t2, err := strconv.Atoi(splitMsg[3])
			checkError(err)
			
			delta := (t1+t3)/2 - t2
			m.Slaves[slaveAddress].Clock = t2
			m.Slaves[slaveAddress].Delta = delta
		}
	}
}

func (m *Master) sendClockRequest(s *Slave) {

	slaveAddr, err := net.ResolveUDPAddr("udp", s.Address)
	checkError(err)

	msg := strconv.Itoa(m.SyncRound) + " request " + strconv.Itoa(m.Clock)
	// fmt.Println("Sending this msg: ", msg)

	outBuf := m.Logger.PrepareSend("Sending message to slave: ", msg)

	// Send a message to the slave requesting its clock
	// _, err = m.Conn.WriteToUDP(outBuf, slaveAddr)
	_, err = capture.WriteTo(m.Conn.WriteTo, outBuf, slaveAddr)
	checkError(err)
}

func (m *Master) sendClockUpdate(s *Slave, syncRound int, clockDiff int) {

	slaveAddr, err := net.ResolveUDPAddr("udp", s.Address)
	checkError(err)

	msg := strconv.Itoa(syncRound) + " update " + strconv.Itoa(s.Clock + clockDiff)
	outBuf := m.Logger.PrepareSend("Sending message to slave: ", &msg)

	// Send a message to the slave requesting its clock
	// _, err = m.Conn.WriteToUDP([]byte(msg), slaveAddr)
	_, err = capture.WriteTo(m.Conn.WriteTo, outBuf, slaveAddr)
	checkError(err)
}

func startClock(clock *int) {
	ticker := time.NewTicker(1 * time.Millisecond)
	
	for _ = range ticker.C {
 	   *clock++
	}
}

func (m *Master) loadSlavesFromFile() {
	// slaves := make([]Slave, 0)

	// Read slavesfile to get list of slaves (host:ip) to connect with
	f, err := os.Open(m.SlavesFile)
	checkError(err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		slave_address := scanner.Text()
		m.Slaves[slave_address] = &Slave{slave_address, -1, -1}
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
