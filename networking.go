package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type Message struct {
	SyncRound int
	Type      string
	Body      json.RawMessage
}

type ClockData struct {
	T1 int
	T2 int
}

func createUDPConn(address string) net.PacketConn {
	conn, err := net.ListenPacket("udp", address)
	checkError(err)

	return conn
}

func sendMessage(conn net.PacketConn, address string, message Message) {
	rAddr, err := net.ResolveUDPAddr("udp", address)
	if checkError(err) {
		return
	}

	messageJSON, err := json.Marshal(&message)
	if err != nil {
		fmt.Println(err)
	}

	_, err = conn.WriteTo(messageJSON, rAddr)
	checkError(err)
}

func readMessage(conn net.PacketConn) (string, Message) {
	b := make([]byte, 1024)
	var message Message

	n, senderAddress, err := conn.ReadFrom(b[0:])
	checkError(err)

	err = json.Unmarshal(b[:n], &message)
	checkError(err)

	return senderAddress.String(), message
}

func newMessage(syncRound int, messageType string, data interface{}) Message {
	message := Message{
		SyncRound: syncRound,
		Type:      messageType,
	}

	if data != nil {
		dataJSON, err := json.Marshal(&data)
		checkError(err)
		message.Body = dataJSON
	}

	return message
}
