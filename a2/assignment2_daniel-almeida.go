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
	"github.com/arcaneiceman/GoVector/capture"
	"github.com/arcaneiceman/GoVector/govec"
)

type Slave struct {
    Address string
    Clock int
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

// var GoVecLogger *govec.GoLog

func (s Slave) SetClock(newClock int) {
	s.Clock = newClock
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
	if err != nil { 
		panic(err)
	}
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
		if err != nil {
		    panic(err)
		}

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
		slave := Slave{ipPort, initialClock}
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
			go m.sendClockRequest(v)
		}

		syncRoundTimeout := time.NewTimer(time.Second * 5)
		<-syncRoundTimeout.C

		// Update syncRound. Messages received from now on will be ignored until the next sendClockRequest
		m.SyncRound++

		// Calculate clock diff
		clockAdjust := 10

		// Send updated clock to each slave
		for _, v := range m.Slaves {
			go m.sendClockUpdate(v, m.SyncRound, clockAdjust)
		}

		for _, v := range m.Slaves {
			fmt.Printf("Slave %v - clock: %v\n", v.Address, v.Clock)
		}
		// From time to time, startSynchronization(syncRound=i, timeout=5seconds):
		// Note: syncRound can be used to ignore replies from earlier sync rounds.
		//     query slaves for their local time (ignore slaves that don't reply)
		//     for each slave,
		//         slaveDeltaValue = computeTimeDelta(localClock, slaveClock)
		//     Compute fault-tolerant avg: avg of largest set of delta values that from one another by at most d
		//     for each slave,
		//         timeCorrection = avg - slaveDeltaValue;
		//         sendTimeCorrection(timeCorrection))
		//     Adjust local time
	}
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
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func (m *Master) listenToSlaves() {
	b := make([]byte, 1024)

	for {

		var messageReceived string
		// n, slaveAddr, err := m.Conn.ReadFromUDP(b[0:])
		n, slaveAddr, err := capture.ReadFrom(m.Conn.ReadFrom, b[0:])
		if err != nil {
            fmt.Println("Error: ", err)
            continue
        }

        
		m.Logger.UnpackReceive("Received message from slave", b[0:n], &messageReceived)
        
        slaveAddress := slaveAddr.String()
        fmt.Println(messageReceived, " from Slave at ", slaveAddress)
		
		splitMsg := strings.Split(messageReceived, " ")

		if len(splitMsg) != 3 {
			fmt.Println("Malformed message from ", slaveAddress)
			continue
		}

		syncRound, err := strconv.Atoi(splitMsg[0])
		if err != nil {
			fmt.Println("Could not parse syncRound")
			continue
		}

		if syncRound < m.SyncRound {
			fmt.Println("Message out syncRound.")
			continue
		}

		action := splitMsg[1]
		slaveClock, err := strconv.Atoi(splitMsg[2])
		if err != nil {
			fmt.Println("Could not parse clock received from Slave.")
			continue
		}

		if action == "clock" {
			// Update slave's clock
			fmt.Printf("Updating slave clock from %v to %v", m.Slaves[slaveAddress].Clock, slaveClock)
			m.Slaves[slaveAddress].Clock = slaveClock
		}
	}
}

func (m *Master) sendClockRequest(s *Slave) {

	slaveAddr, err := net.ResolveUDPAddr("udp", s.Address)
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := strconv.Itoa(m.SyncRound) + " request "
	// fmt.Println("Sending this msg: ", msg)

	outBuf := m.Logger.PrepareSend("Sending message to slave: ", msg)

	// Send a message to the slave requesting its clock
	// _, err = m.Conn.WriteToUDP(outBuf, slaveAddr)
	_, err = capture.WriteTo(m.Conn.WriteTo, outBuf, slaveAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (m *Master) sendClockUpdate(s *Slave, syncRound int, clockDiff int) {

	slaveAddr, err := net.ResolveUDPAddr("udp", s.Address)
	if err != nil {
		fmt.Println(err)
		return
	}

	msg := strconv.Itoa(syncRound) + " update " + strconv.Itoa(s.Clock + clockDiff)
	outBuf := m.Logger.PrepareSend("Sending message to slave: ", &msg)

	// Send a message to the slave requesting its clock
	// _, err = m.Conn.WriteToUDP([]byte(msg), slaveAddr)
	_, err = capture.WriteTo(m.Conn.WriteTo, outBuf, slaveAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
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
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer conn.Close()

	// Create byte array with underlying array to hold up to 1024 bytes
	b := make([]byte, 1024)
	
	lastKnownSyncRound := -1

	for {
		var msgReceived string
		// Receive message from Master
		// n, masterAddress, err := conn.ReadFromUDP(b[:])
		n, masterAddress, err := capture.ReadFrom(conn.ReadFrom, b[0:])
		if err != nil {
			fmt.Println(err)
			continue
		}

		Logger.UnpackReceive("Received message from master: ", b[0:n], &msgReceived)
		// fmt.Println("Received ", string(b[:n]), "from master at ", masterAddress)

		// msgReceived := string(b[:n])
		splitMsg := strings.Split(msgReceived, " ")

		if len(splitMsg) < 2 || len(splitMsg) > 3 {
			fmt.Println("Malformed message from ", masterAddress)
			continue
		}

		syncRound := splitMsg[0]
		syncRoundInt, err := strconv.Atoi(syncRound)
		if err != nil {
			fmt.Println("Malformed message from master.")
			continue
		}

		action := splitMsg[1]

		if action == "request" {
			lastKnownSyncRound = syncRoundInt

			// Send local clock back to Master
			responseMsg := syncRound + " clock " + strconv.Itoa(s.Clock)
			msgBuf := Logger.PrepareSend("Sending message to master: ", responseMsg)

			// n, err = conn.WriteToUDP([]byte(msgBuf), masterAddress)
			_, err = capture.WriteTo(conn.WriteTo, msgBuf, masterAddress)
			if err != nil {
				fmt.Println(err)
				continue
			}

		} else if action == "update"  && syncRoundInt >= lastKnownSyncRound{
			// Update local clock (sync)
			newClock, err := strconv.Atoi(splitMsg[2])
			if err != nil {
				fmt.Println("Could not parse new clock received from Master.")
				continue
			}
			s.SetClock(newClock)
			fmt.Println("Updated clock to: ", s.Clock)
		}
		
	}
	// Listen for time correction and update local clock
	
	fmt.Println("Hello world.")	
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
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		slave_address := scanner.Text()
		m.Slaves[slave_address] = &Slave{slave_address, -1}
		// slaves = append(slaves, slave_address, -1})
	}
	// return slaves
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
