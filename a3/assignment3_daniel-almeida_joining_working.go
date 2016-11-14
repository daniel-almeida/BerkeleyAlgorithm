/* CPSC 538B - Distributed Systems. Daniel Almeida - Assignment 3 */

package main

import (
	"fmt"
	"os"
	"strconv"
	"net"
	"encoding/json"
	"math/rand"
	"time"
	"sync"
	// "github.com/arcaneiceman/GoVector/capture"
	// "github.com/arcaneiceman/GoVector/govec"
)

var (
	ModeFlag string
	ipForPortalNode string
	ipForListening string
	RTT int
	FlipProb float64
	FlipInvokeCS int
	CSSleepTime int
	shivizLogfile string
	dinvLogfile string
)

var (
	me int
	N int // Number of nodes in the network
	Nodes []Node = make([]Node, 0)
	ourSequenceNumber int
	highestSequenceNumber int
	outstandingReplyCount int
	requestingCS bool = false
)

type Node struct {
	NodeId int
	Address string
	ReplyDeferred bool
}

type Message struct {
	Type string //`json:type,string`
	SenderId int
	SenderAddress string
	Data json.RawMessage
}

type RequestData struct {
	SequenceNumber int
}

type JoinData struct {
	Address string //`json:address,string`
}

type AcceptData struct {
	ActiveNodes []Node
	AssignedNodeId int
	HighestSequenceNumber int
}


// Parses all Args
func parseArgs() {
	var err error

	if len(os.Args) < 2 {
		panic("Insufficient arguments.")
	}

	ModeFlag = os.Args[1]
	if ModeFlag == "-b" {
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
		// fmt.Printf("\nSanity check: running in bootstrap mode with IP for listening (%v), RTT (%v), FlipeProb (%v), FlipInvokeCS (%v), CSSleepTime (%v), shiviz-logfile (%v), and dinv-logfile (%v)\n", ipForListening, RTT, FlipProb, FlipInvokeCS, CSSleepTime, shivizLogfile, dinvLogfile)
	} else if ModeFlag == "-j" {
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
		// fmt.Printf("\nSanity check: running in bootstrap mode with IP for for Portal Node (%v), IP for listening (%v), RTT (%v), FlipeProb (%v), FlipInvokeCS (%v), CSSleepTime (%v), shiviz-logfile (%v), and dinv-logfile (%v)\n", ipForPortalNode, ipForListening, RTT, FlipProb, FlipInvokeCS, CSSleepTime, shivizLogfile, dinvLogfile)
	} else {
		panic("Invalid flag. Use -b or -j")
	}
}

var mutex = &sync.Mutex{}
var done chan int = make(chan int, 1)
var joined chan int = make(chan int, 1)
var conn net.PacketConn
var err error

func main() {
	parseArgs()

	if ModeFlag == "-b" {
		run(true)
	} else if ModeFlag == "-j" {
		run(false)
	}

	fmt.Println("Hello world.")
}

func run(isBootstrap bool) {
	conn, err = net.ListenPacket("udp", ipForListening)
	checkError(err)

	if isBootstrap {
		thisNode := Node{0, ipForListening, false}
		Nodes = append(Nodes, thisNode)
		N = 1
		fmt.Printf("\nInitialized Nodes with thisNode: %+v", Nodes)
		go startListener()
		go invokeCS()
		<-done
	} else {
		// Connect with Portal Node
		go startListener()
		go sendJoinMessage()
		<- joined
		go invokeCS()
		<-done
	}
}

func invokeCS() {
	fmt.Printf("\n\nEntered invokeCS")
	rand.Seed(time.Now().UnixNano())

	for {
		fmt.Printf("\nSleeping for %v before trying to invoke CS", FlipInvokeCS)
		time.Sleep(time.Millisecond * time.Duration(FlipInvokeCS))
		f := rand.Float64()
		fmt.Printf("\nFlipped %v (FlipProb is %v)", f, FlipProb)
		fmt.Printf("\nRequestingCS is %v", requestingCS)
		if requestingCS == false && f < FlipProb {
			mutex.Lock()
			requestingCS = true
			ourSequenceNumber = highestSequenceNumber + 1
			mutex.Unlock()

			outstandingReplyCount = N-1
			fmt.Printf("\nNode #%v trying to enter CS with sequence number %v", me, ourSequenceNumber)
			// Send REQUEST messages to all other nodes
			for _, node := range Nodes {
				if node.NodeId != me {
					fmt.Printf("\nSending REQUEST to Node #%v at %v", node.NodeId, node.Address)
					sendRequestMessage(node, ourSequenceNumber)
				}
			}
			// Wait for REPLY from all other nodes
			fmt.Printf("\nOutstanding Reply Count: %v\n", outstandingReplyCount)
			for outstandingReplyCount > 0 { 
				// fmt.Printf("%v", outstandingReplyCount)
			}
			// Enter CS
			fmt.Printf("\nNode #%v executing CS now...", me)
			time.Sleep(time.Millisecond * time.Duration(CSSleepTime)) // CS

			requestingCS = false
			for _, node := range Nodes {
				if node.NodeId != me && node.ReplyDeferred == true {
					sendReplyMessage(node)
					node.ReplyDeferred = false
				}
			}
		}
	}
}

func sendNewNodeMessage(node Node) {
	data := RequestData{}
	requestData, err := json.Marshal(&data)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "NEW_NODE",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: requestData,
	}

	newNodeMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("\n\nMarshalled data: %v\n", string(newNodeMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(newNodeMessage, rAddr)
}

func sendNewNodeConfirmMessage(node Node) {
	myHighestSequenceNum := highestSequenceNumber
	data, err := json.Marshal(&myHighestSequenceNum)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "CONFIRM_NEW_NODE",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: data,
	}

	newNodeConfirmMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("\n\nMarshalled data: %v\n", string(newNodeConfirmMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(newNodeConfirmMessage, rAddr)
}

func handlenewNodeConfirmMessage(newNodeConfirmMessage Message, conn net.PacketConn) {
	var receivedHighestSequenceNumber int
	err := json.Unmarshal(newNodeConfirmMessage.Data, &receivedHighestSequenceNumber)
	checkError(err)

	fmt.Printf("\nReceived NEW_NODE_CONFIRM from Node #%v with HighestSequenceNumber %v", newNodeConfirmMessage.SenderId, receivedHighestSequenceNumber)
	if receivedHighestSequenceNumber >= highestSequenceNumber {
		highestSequenceNumber = receivedHighestSequenceNumber
	}
}

func handleNewNodeMessage(newNodeMessage Message, conn net.PacketConn) {
	found := false
	for _, node := range Nodes {
		if node.NodeId == me {
			continue
		}

		if node.Address == newNodeMessage.SenderAddress && node.NodeId == newNodeMessage.SenderId {
			found = true
			break
		}
	}
	if found == false {
		newNode := Node{
			NodeId: newNodeMessage.SenderId,
			Address: newNodeMessage.SenderAddress,
			ReplyDeferred: false,
		}
		Nodes = append(Nodes, newNode)
	}
}

func sendReplyMessage(node Node) {
	data := RequestData{}
	requestData, err := json.Marshal(&data)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "REPLY",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: requestData,
	}

	replyMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("\n\nMarshalled data: %v\n", string(replyMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(replyMessage, rAddr)
}

func sendRequestMessage (node Node, sequenceNumber int) {
	requestData := RequestData{sequenceNumber}
	
	data, err := json.Marshal(&requestData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("\n\nMarshalled JoingMessageData: %v\n", string(data))

	msg := Message{
		Type: "REQUEST",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: data,
	}

	requestMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("\n\nMarshalled data: %v\n", string(requestMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(requestMessage, rAddr)
}

func startListener() {
	// lAddr, err := net.ResolveUDPAddr("udp", ipForListening)
	// checkError(err)

	b := make([]byte, 1024)
	for {
		var incomingMessage Message
		n, _, err := conn.ReadFrom(b)
		if err != nil {
			fmt.Println(err)
			continue
		}

		err = json.Unmarshal(b[:n], &incomingMessage)
		// fmt.Printf("\n\nReceived msg after Unmarshal: %+v\n", msgReceived)

		switch incomingMessage.Type {
    		case "JOIN":
    			// fmt.Printf("\n\nReceived a JOIN message.\n")
				go handleJoinMessage(incomingMessage, conn)
			case "ACCEPT":
				// fmt.Printf("\n\nReceived an ACCEPT message.")
				go handleAcceptMessage(incomingMessage, conn)
			case "REQUEST":
				// fmt.Printf("\n\nReceived a REQUEST message.")
				go handleRequestMessage(incomingMessage, conn)
			case "REPLY":
				// fmt.Printf("\n\nReceived a REPLY message.")
				go handleReplyMessage(incomingMessage, conn)
			case "NEW_NODE":
				go handleNewNodeMessage(incomingMessage, conn)
			case "CONFIRM_NEW_NODE":
				go handlenewNodeConfirmMessage(incomingMessage, conn)
		}
	}
	done <- 1
}

func handleReplyMessage(replyMessage Message, conn net.PacketConn) {
	outstandingReplyCount = outstandingReplyCount - 1
	fmt.Printf("\n\nReceived REPLY from Node #%v", replyMessage.SenderId)	
}

func handleRequestMessage(requestMessage Message, conn net.PacketConn) {
	var data RequestData
	err := json.Unmarshal(requestMessage.Data, &data)
	checkError(err)

	fmt.Printf("\nReceived REQUEST from Node #%v with SequenceNumber %v", requestMessage.SenderId, data.SequenceNumber)
	fmt.Printf("\nMy Node #%v and SequenceNumber %v", me, ourSequenceNumber)
	if data.SequenceNumber >= highestSequenceNumber {
		highestSequenceNumber = data.SequenceNumber	
	}
	mutex.Lock()
	deferRequest := false
	if requestingCS && ((data.SequenceNumber > ourSequenceNumber) || (data.SequenceNumber == ourSequenceNumber && requestMessage.SenderId > me)) {
		deferRequest = true
	}
	mutex.Unlock()

	var idx int
	for i, n := range Nodes {
		if n.NodeId == requestMessage.SenderId {
			idx = i
		}
	}

	if deferRequest == true {
		Nodes[idx].ReplyDeferred = true
	} else {
		go sendReplyMessage(Nodes[idx])
	}
	// fmt.Printf("\nChanged ReplyDeferred of Node %+v to %+v", Nodes[idx], deferRequest)
	// fmt.Printf("\nNodes: %+v", Nodes)
}

func handleJoinMessage(joinMessage Message, conn net.PacketConn) {
	newNodeIP := joinMessage.SenderAddress
	
	idxNewNode := len(Nodes)
	// fmt.Printf("\n\nlenght of Nodes slice (and index of newNode): %v", idxNewNode)
	for idx, node := range Nodes {
		if &node == nil {
			idxNewNode = idx
		}
	}

	newNode := Node{idxNewNode, newNodeIP, false}
	fmt.Printf("\n\nNew node: %+v", newNode)
	Nodes = append(Nodes, newNode)
	fmt.Printf("\nList of nodes: %+v", Nodes)
	N = len(Nodes)

	acceptData := AcceptData{
		AssignedNodeId: idxNewNode,
		ActiveNodes: Nodes,
		HighestSequenceNumber: highestSequenceNumber,
	}

	acceptDataJson, err := json.Marshal(acceptData)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "ACCEPT",
		SenderId: me,
		SenderAddress: ipForListening,
		Data: acceptDataJson,
	}

	acceptMessageJson, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("\n\nMarshalled data: %v\n", string(acceptMessageJson))

	rAddr, err := net.ResolveUDPAddr("udp", newNodeIP)
	checkError(err)
	conn.WriteTo(acceptMessageJson, rAddr)	
}

func handleAcceptMessage(acceptMessage Message, conn net.PacketConn) {
	var data AcceptData
	err := json.Unmarshal(acceptMessage.Data, &data)
	checkError(err)

	Nodes = data.ActiveNodes
	me = data.AssignedNodeId
	N = len(Nodes)
	highestSequenceNumber = data.HighestSequenceNumber
	for _, node := range Nodes {
		if node.NodeId != me {
			go sendNewNodeMessage(node)	
		}
	}
	fmt.Printf("\n\nFinal status of Joining Node\nNumber of nodes: %+v; \nNodes: %+v; \nI am Node #%v and highestSequenceNumber %v", N, Nodes, me, highestSequenceNumber)
	joined <- 1
}

func sendJoinMessage() {
	JoinMessageData := JoinData{ipForListening}
	
	data, err := json.Marshal(&JoinMessageData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("\n\nMarshalled JoingMessageData: %v\n", string(data))

	msg := Message{
		Type: "JOIN",
		SenderAddress: ipForListening, 
		Data: data,
	}

	joinData, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("\n\nMarshalled data: %v\n", string(joinData))
	rAddr, err := net.ResolveUDPAddr("udp", ipForPortalNode)
	conn.WriteTo(joinData, rAddr)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}