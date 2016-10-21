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
)

type Slave struct {
    address string
    clockTime int64
}

func (s *Slave) SetClock(newClock int64) {
	s.clockTime = newClock
}

func updateClock(s *Slave) {
    fmt.Println("Requested clock...")
    ch := make(chan int64, 1)

	go func() {
		// Connect over UDP and requesting clock...
	    time.Sleep(1 * time.Second)
	    ch <- 1
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

func runMaster(clock int64) {
	fmt.Println("Running master...")
	// var clock int64 = time.Now().Unix()
	// fmt.Println("Initial clock: ", clock)
	
	go startClock(&clock)
	
	slaves := loadSlavesFromFile("slavesfile")
	fmt.Println(slaves)

	// res := make([]int, len(slaves))
	for i, _ := range slaves {
		updateClock(&slaves[i])
	}

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

	
}

func runSlave(clock int64) {
	fmt.Println("Running slave...")
	fmt.Println("Hello world.")
	// Connect with master at host:ip
	// Keep track of last known sync round.
	// Ignore messages with sync round smaller than the last known sync round (delayed messages)
	// Listen for requests for local time and reply with local time
	// Listen for time correction and update local clock
}

func handleError(err error) {
	fmt.Println(err)
	return
}

func main() {
	// Check number of arguments
	// numArgs := len(os.Args)
	// fmt.Println(numArgs)

	// Parse flag (master or slave)
	masterOrSlaveFlag := os.Args[1]
	fmt.Println("Master of slave: ", masterOrSlaveFlag)


	// Parse initial value of clock as int64
	initialClockString := os.Args[2]
	initialClock, err := strconv.ParseInt(initialClockString, 10, 64)
	if err != nil {
	    panic(err)
	}
	fmt.Println("Initial clock: ", initialClock)

	if masterOrSlaveFlag == "-m" {
		runMaster(initialClock)
	} else if masterOrSlaveFlag == "-s" {
		runSlave(initialClock)
	}
}
