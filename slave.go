package main

import (
	"encoding/json"
	"fmt"
)

type Slave struct {
	Address string
	Clock   int
	Delta   int
}

type Slaves []Slave

func (slice Slaves) Len() int {
	return len(slice)
}

func (slice Slaves) Less(i, j int) bool {
	return slice[i].Clock < slice[j].Clock
}

func (slice Slaves) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (s *Slave) run() {
	go startClock(&s.Clock)

	// Set up UDPConn
	conn := createUDPConn(s.Address)
	defer conn.Close()

	lastKnownSyncRound := -1
	for {
		senderAddress, message := readMessage(conn)
		// fmt.Println(message, " received from Master at ", senderAddress)

		switch message.Type {
		case "CLOCK_REQUEST":
			// Master is requesting local clock
			fmt.Println("Received CLOCK_REQUEST from Master")
			lastKnownSyncRound = message.SyncRound
			var t1 int // master's clock
			err := json.Unmarshal(message.Body, &t1)
			if err != nil {
				fmt.Println(err)
				continue
			}
			t2 := s.Clock // local clock

			// Send local clock back to Master
			clockResponseMessage := newMessage(message.SyncRound, "CLOCK_RESPONSE", ClockData{T1: t1, T2: t2})
			sendMessage(conn, senderAddress, clockResponseMessage)
		case "CLOCK_UPDATE":
			if message.SyncRound < lastKnownSyncRound {
				// Late message from an earlier synchronization round
				continue
			}

			fmt.Println("Received CLOCK_UPDATE from Master")
			// Update local clock (sync)
			var clockOffset int
			err := json.Unmarshal(message.Body, &clockOffset)
			if err != nil {
				fmt.Println(err)
				continue
			}
			s.Clock = s.Clock + clockOffset
			fmt.Printf("Clock %v adjusted (offset: %v) to %v\n", s.Clock, clockOffset, s.Clock+clockOffset)
		}
	}

	fmt.Println("Hello world.")
}
