/* CPSC 538B - Distributed Systems. Daniel Almeida - Assignment 3 */

package main

import (
	"fmt"
	"os"
	"strconv"
	"net"
)

var (
	ipForPortalNode string
	ipForListening string
	RTT int
	FlipProb float64
	FlipInvokeCS int
	CSSleepTime int
	shivizLogfile string
	dinvLogfile string
)

func main() {
	var err error

	if len(os.Args) < 2 {
		panic("Insufficient arguments.")
	}

	modeFlag := os.Args[1]
	if modeFlag == "-b" {
		if len(os.Args) < 8 {
			fmt.Printf("Args: %v\n", os.Args[1:])
			panic("Insufficient arguments.")
		}
		// Boostrap mode
		ipForListening = os.Args[2]

		RTT, err = strconv.Atoi(os.Args[3])
		checkError(err)

		FlipProb, err = strconv.ParseFloat(os.Args[4], 64)
		checkError(err)

		FlipInvokeCS, err  = strconv.Atoi(os.Args[5])
		checkError(err)

		CSSleepTime, err = strconv.Atoi(os.Args[6])
		checkError(err)

		shivizLogfile = os.Args[7]
		dinvLogfile = os.Args[8]
		fmt.Printf("\nSanity check: running in bootstrap mode with IP for listening (%v), RTT (%v), FlipeProb (%v), FlipInvokeCS (%v), CSSleepTime (%v), shiviz-logfile (%v), and dinv-logfile (%v)\n", ipForListening, RTT, FlipProb, FlipInvokeCS, CSSleepTime, shivizLogfile, dinvLogfile)
		run(true)
	} else if modeFlag == "-j" {
		if len(os.Args) < 9 {
			fmt.Printf("Args: %v\n", os.Args[1:])
			panic("Insufficient arguments.")
		}
		// Joining mode
		ipForPortalNode = os.Args[2]
		ipForListening = os.Args[3]

		RTT, err = strconv.Atoi(os.Args[4])
		checkError(err)

		FlipProb, err = strconv.ParseFloat(os.Args[5], 64)
		checkError(err)

		FlipInvokeCS, err = strconv.Atoi(os.Args[6])
		checkError(err)

		CSSleepTime, err = strconv.Atoi(os.Args[7])
		checkError(err)

		shivizLogfile = os.Args[8]
		dinvLogfile = os.Args[9]
		fmt.Printf("\nSanity check: running in bootstrap mode with IP for for Portal Node (%v), IP for listening (%v), RTT (%v), FlipeProb (%v), FlipInvokeCS (%v), CSSleepTime (%v), shiviz-logfile (%v), and dinv-logfile (%v)\n", ipForPortalNode, ipForListening, RTT, FlipProb, FlipInvokeCS, CSSleepTime, shivizLogfile, dinvLogfile)
		run(false)
	} else {
		panic("Invalid flag. Use -b or -j")
	}

	fmt.Println("Hello world.")

}

func run(isBootstrap bool) {
	if isBootstrap {
		runPortalServer()
	} else {
		// Connect with Portal Node
		contactPortalNode()
		fmt.Println("Joining node says Hello World.")
	}
}

func contactPortalNode() {
	conn, err := net.Dial("tcp", ipForPortalNode)
	checkError(err)

	defer conn.Close()
	conn.Write([]byte("Sanity check.")) // don't care about return value
}

func runPortalServer() {
	server, err := net.Listen("tcp", ipForListening)
	checkError(err)

	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handlePortalConn(conn)	
	}
}

func handlePortalConn(conn net.Conn) {
	var msgReceived string
	b := make([]byte, 1024)
	
	defer conn.Close()
	
	n, err := conn.Read(b[0:])
	checkError(err)

	msgReceived = string(b[:n])
	fmt.Printf("\n\nMessage received from node: %v", msgReceived)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}