package main

import (
	"fmt"
	"os"
	"net"
)

func main() {
	ipPort := os.Args[1]

	// Listen to host:ip
	slaveAddr, err := net.ResolveUDPAddr("udp", ipPort)
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
	
	for {
		// Receive message from Master
		n, masterAddress, err := slaveConn.ReadFromUDP(b[0:])
		fmt.Println("Received ", string(b[:n]), "from master at ", masterAddress)


		// Write clock back to master
		n, err = slaveConn.WriteToUDP([]byte("sure"), masterAddress)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

