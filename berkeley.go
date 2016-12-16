/*
Periodically, the master queries all of the slave nodes for their local time and
each node replies with their current time.  The master then computes the time
difference between its local clock and that of each of the slaves (deriving a
set of delta values, one per slave). Next, the master computes avg, a fault
tolerant average of the delta values: an average over the largest set of delta
values that differ from one another by at most a pre-defined constant d. Then,
for each slave the master computes a time correction value by computing the
difference between avg and the delta value for the slave. Finally, the master
sends this correction value to each slave for the slaves to adjust their clock.
*/

package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/arcaneiceman/GoVector/govec"
)

var Logger *govec.GoLog

func main() {
	// Check number of arguments
	numArgs := len(os.Args)
	if numArgs < 2 || numArgs > 6 {
		panic("Not enough arguments.")
	}

	address := os.Args[2]

	initialClockString := os.Args[3]
	initialClock, err := strconv.Atoi(initialClockString)
	checkError(err)

	// Parse flag (master or slave)
	masterOrSlaveFlag := os.Args[1]
	if masterOrSlaveFlag == "-m" {
		// Master is running
		if numArgs != 6 {
			panic("Expected 5 arguments.")
		}

		thresholdString := os.Args[4]
		threshold, err := strconv.Atoi(thresholdString)
		checkError(err)

		slavesFile := os.Args[5]
		master := Master{address, initialClock, threshold, make(map[string]*Slave), 0}
		master.loadSlavesFromFile(slavesFile)
		master.run()

	} else if masterOrSlaveFlag == "-s" {
		// Slave is running
		if numArgs != 4 {
			panic("Expected 3 arguments.")
		}

		slave := Slave{address, initialClock, -1}
		slave.run()
	} else {
		panic("Flag should be -m or -s")
	}
}

func checkError(err error) bool {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
		return true
	}
	return false
}
