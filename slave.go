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
	"net"
	"strconv"
	"strings"

	"github.com/arcaneiceman/GoVector/capture"
	"github.com/arcaneiceman/GoVector/govec"
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

		} else if action == "update" && syncRoundInt >= lastKnownSyncRound {
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
