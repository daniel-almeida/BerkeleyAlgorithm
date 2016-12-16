package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/arcaneiceman/GoVector/capture"
	"github.com/arcaneiceman/GoVector/govec"
)

type Master struct {
	Address    string
	Clock      int
	Threshold  int
	SlavesFile string
	LogFile    string
	Slaves     map[string]*Slave
	// Conn *net.UDPConn
	Conn      net.PacketConn
	SyncRound int
	Logger    *govec.GoLog
	// slaves []Slave
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
			avg = totalDelta / sizeOfSubset
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

	msg := strconv.Itoa(syncRound) + " update " + strconv.Itoa(s.Clock+clockDiff)
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
		fmt.Println("Created slave from file: ", m.Slaves[slave_address])
	}
}
