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

func (s *Slave) run(logFile string) {
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
