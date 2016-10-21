package main

import (
	"fmt"
	"os"
	"net"
	"time"
)

func listen(conn *net.UDPConn) {
	fmt.Println("Listening...")
	b := make([]byte, 1024)
	for {
		n, slaveAddress, err := conn.ReadFromUDP(b[0:])
		fmt.Println("Received ", string(b[:n]), " from Slave at ", slaveAddress)

		if err != nil {
            fmt.Println("Error: ", err)
        } 
	}
	
}

func sendMessage(conn *net.UDPConn,destAddress string, msg string) {
	destAddr, err := net.ResolveUDPAddr("udp", destAddress)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = conn.WriteToUDP([]byte(msg), destAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Send a message to the slave requesting its clock
	// _, err = conn.Write([]byte(msg))
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
}

func main() {
	// Listen to host:ip
	masterAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10110")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	masterConn, err := net.ListenUDP("udp", masterAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	go listen(masterConn)
	for {
		go sendMessage(masterConn, "127.0.0.1:10111", "gimme clock!")
		go sendMessage(masterConn, "127.0.0.1:10112", "gimme clock!")
		go sendMessage(masterConn, "127.0.0.1:10113", "gimme clock!")	
		syncRoundTimeout := time.NewTimer(time.Second * 5)
		<-syncRoundTimeout.C
	}
	// syncRoundTimeout := time.NewTimer(time.Second * 5)
	// <-syncRoundTimeout.C
	// sendMessage("127.0.0.1:10112", "gimme clock!")
	// sendMessage("127.0.0.1:10113", "gimme clock!")

}