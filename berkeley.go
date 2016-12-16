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
	"strconv"
)

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

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
