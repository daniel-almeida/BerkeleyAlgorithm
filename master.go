package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sort"
	"time"
)

type Master struct {
	listenAddress string
	Clock         int
	Threshold     int
	Slaves        map[string]*Slave
	SyncRound     int
}

var conn net.PacketConn

func (m *Master) run() {
	go startClock(&m.Clock)

	// Set up UDPConn
	conn = createUDPConn(m.listenAddress)
	defer conn.Close()

	// Start listener to hear back from Slaves
	go m.listenToSlaves()

	for {
		requestMessage := newMessage(m.SyncRound, "CLOCK_REQUEST", m.Clock)
		for _, v := range m.Slaves {
			v.Clock = -1
			go sendMessage(conn, v.Address, requestMessage)
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
			// fmt.Printf("Sending clock adjust. AVG: %v ; Delta: %v; Final value: %v\n\n", avg, v.Delta, v.Delta-avg)
			offset := v.Delta - avg
			go sendMessage(conn, v.Address, newMessage(m.SyncRound, "CLOCK_UPDATE", offset))
		}

		// Update Master's clock as well
		m.Clock = m.Clock - avg
	}
}

func calculateAvg(slavesAlive Slaves, d int) int {
	sort.Sort(slavesAlive)
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
	// fmt.Println("Size of subset: ", sizeLargestSubset)
	// fmt.Println("Avg delta: ", avg)
	return avg
}

func (m *Master) listenToSlaves() {
	for {
		senderAddress, message := readMessage(conn)
		t3 := m.Clock
		// fmt.Println(message, " received from Slave at ", senderAddress)

		if message.Type != "CLOCK_RESPONSE" {
			fmt.Println("Received invalid message.")
			continue
		}

		if message.SyncRound < m.SyncRound {
			fmt.Println("Message out syncRound.")
			continue
		}

		var data ClockData
		err := json.Unmarshal(message.Body, &data)
		if err != nil {
			fmt.Println(err)
			continue
		}

		delta := (data.T1+t3)/2 - data.T2
		m.Slaves[senderAddress].Clock = data.T2
		m.Slaves[senderAddress].Delta = delta
		fmt.Printf("--- SYNC %v (SLAVE %v) --- \n", message.SyncRound, senderAddress)
		fmt.Printf("--> CLOCK: %v; \n--> OFFSET: %v; \n--> UPDATED CLOCK: %v\n\n", data.T2, delta, data.T2+delta)
	}
}

func startClock(clock *int) {
	ticker := time.NewTicker(1 * time.Millisecond)

	for _ = range ticker.C {
		*clock++
	}
}

func (m *Master) loadSlavesFromFile(path string) {
	// Read slavesfile to get list of slaves (host:ip) to connect with
	f, err := os.Open(path)
	checkError(err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		slaveAddress := scanner.Text()
		m.Slaves[slaveAddress] = &Slave{slaveAddress, -1, -1}
	}
}
