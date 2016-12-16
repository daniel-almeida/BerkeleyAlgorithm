package main

import (
	"encoding/json"
	"net"

	"github.com/arcaneiceman/GoVector/capture"
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
	// fmt.Printf("Sending message of type %v to Node %v\n", messageType, address)
	rAddr, err := net.ResolveUDPAddr("udp", address)
	if checkError(err) {
		return
	}

	// conn, err := net.DialUDP("udp", nil, rAddr)
	// if checkError(err) {
	// 	return
	// }

	buf := Logger.PrepareSend("Sending message", message)
	_, err = capture.WriteTo(conn.WriteTo, buf, rAddr)
	checkError(err)
}

func readMessage(conn net.PacketConn) (string, Message) {
	b := make([]byte, 1024)
	var message Message

	n, senderAddress, err := capture.ReadFrom(conn.ReadFrom, b[0:])
	checkError(err)
	Logger.UnpackReceive("Received message", b[0:n], &message)

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
