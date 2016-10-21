/*
* CPSC 538B - Distributed Systems
* Daniel Almeida - Assignment 1
*/

package main

import (
	"fmt"
	"net"
)

func handleError(err error) {
	fmt.Println(err)
	return
}

func main() {
	path := "198.162.52.146:11235"

	// Establish udp connection
	conn, err := net.Dial("udp", path)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	// Send a message to the server before requesting a fortune
	n, err := conn.Write([]byte("fortune please"))

	if err != nil {
		handleError(err)
		return
	}

	// Create byte array with underlying array to hold up to 1024 bytes
	b := make([]byte, 1024)

	n, err = conn.Read(b)
	if err != nil {
		handleError(err)
		return
	}

	fmt.Println(string(b[:n]))
}