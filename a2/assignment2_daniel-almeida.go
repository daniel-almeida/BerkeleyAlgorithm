/* CPSC 538B - Distributed Systems * Daniel Almeida - Assignment 2 */

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
)

type Slave struct {
    address string
    clockTime int64
}

func (s *Slave) SetClock(newClock int64) {
	s.clockTime = newClock
}

func main() {
	// Check number of arguments
	numArgs := len(os.Args)
	fmt.Println(numArgs)
	if numArgs < 2 || numArgs > 7 { 
		panic("Not enough arguments.") 
	}

	ipPort := os.Args[2]
	fmt.Println("ip:port -> ", ipPort)

	initialClockString := os.Args[3]
	initialClock, err := strconv.ParseInt(initialClockString, 10, 64)
	if err != nil { 
		panic(err)
	}
	fmt.Println("Initial clock: ", initialClock)

	// Parse flag (master or slave)
	masterOrSlaveFlag := os.Args[1]
	if masterOrSlaveFlag == "-m" {
		// Master is running
		if numArgs != 7 {
			panic("Expected 6 arguments.")
		}

		thresholdString := os.Args[4]
		threshold, err := strconv.ParseInt(thresholdString, 10, 64)
		if err != nil {
		    panic(err)
		}

		slavesFile := os.Args[5]
		logFile := os.Args[6]

		runMaster(ipPort, initialClock, threshold, slavesFile, logFile)

	} else if masterOrSlaveFlag == "-s" {
		// Slave is running
		if numArgs != 5 {
			panic("Expected 4 arguments.")
		}

		logFile := os.Args[4]
		runSlave(ipPort, initialClock, logFile)
	} else {
		panic("Flag should be -m or -s")
	}
}

func runMaster(address string, clock int64, threshold int64, slavesFile string, logFile string) {
	fmt.Println("Running master...")
	// var clock int64 = time.Now().Unix()
	// fmt.Println("Initial clock: ", clock)
	
	go startClock(&clock)
	
	slaves := loadSlavesFromFile(slavesFile)
	fmt.Println(slaves)

	// var responseChans = [5]chan int64
	// for i := range responseChans {
	// 	responseChans[i] := make(chan int64)
	// }
	// res := make([]int, len(slaves))
	syncRound := 0
	for {
		for i, _ := range slaves {
			go updateClock(&slaves[i])
		}

		syncRoundTimeout := time.NewTimer(time.Second * 5)
		<-syncRoundTimeout.C

		for _, slave := range slaves {
			fmt.Printf("Slave %v - clock: %v\n", slave.address, slave.clockTime)
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

		fmt.Println("Final clock: ", clock)

		fmt.Println("Hello world.")
		syncRound++
	}
}

func runSlave(address string, clock int64, logFile string) {
	fmt.Printf("Running slave. Adress %v; clock %v; logFile %v", address, clock, logFile)
	go startClock(&clock)
	// lastKnownSyncRound := -1
		

	// Listen to host:ip
	slaveAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	slaveConn, err := net.ListenUDP("udp", slaveAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer slaveConn.Close()

	// Create byte array with underlying array to hold up to 1024 bytes
	b := make([]byte, 1024)
	
	// Receive message from Master
	n, masterAddress, err := slaveConn.ReadFromUDP(b[0:])
	fmt.Println("Received ", string(b[:n]), "from master at ", masterAddress)


	// Write clock back to master
	n, err = slaveConn.WriteToUDP([]byte(strconv.FormatInt(clock, 10)), masterAddress)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Listen for time correction and update local clock
	
	fmt.Println("Hello world.")	
}

func updateClock(s *Slave) {
    fmt.Println("Requested clock...")
    ch := make(chan int64, 1)

	go func() {
		// Connect over UDP and requesting clock...
	    time.Sleep(1 * time.Second)
	    ch <- requestClockFromSlave(s.address)
	}()

    select {
	    case clock := <-ch:
	    	s.SetClock(clock)
	        fmt.Println("Received clock: ", s.clockTime)
	    case <-time.After(time.Second * 3):
	        s.SetClock(-1)
	        fmt.Printf("Slave %v timeout. Clock set to %v\n", s.address, s.clockTime)
    }
}

func requestClockFromSlave(path string) int64 {
	conn, err := net.Dial("udp", path)
	if err != nil {
		fmt.Println(err)
		return -1
	}

	defer conn.Close()

	// Send a message to the slave requesting its clock
	n, err := conn.Write([]byte("clock please"))

	if err != nil {
		fmt.Println(err)
		return -1
	}

	// Create byte array with underlying array to hold up to 1024 bytes
	b := make([]byte, 1024)

	n, err = conn.Read(b)
	if err != nil {
		fmt.Println(err)
		return -1
	}

	clockFromSlave, err := strconv.ParseInt(string(b[:n]), 10, 64)
	if err != nil {
	    fmt.Println(err)
		return -1
	}

	return clockFromSlave
}

func startClock(clock *int64) {
	ticker := time.NewTicker(1 * time.Millisecond)
	
	for _ = range ticker.C {
 	   *clock++
 	   // fmt.Println("New clock: ", clock, " at ", t)
	}
}

func loadSlavesFromFile(filepath string) []Slave {
	slaves := make([]Slave, 0)

	// Read slavesfile to get list of slaves (host:ip) to connect with
	f, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		slaves = append(slaves, Slave{scanner.Text(), -1})
	}
	return slaves
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
