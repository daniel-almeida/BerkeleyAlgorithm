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
	highestNodeId int
	outstandingReplyCount int
	requestingCS bool = false
	joinedNetwork bool = false
)

type Node struct {
	NodeId int
	Address string
	ReplyDeferred bool
	Alive bool
	AwaitingReply bool
	IsThere bool
}

func findNode(nodeId int) *Node {
	for idx, node := range Nodes {
		if node.NodeId == nodeId {
			return &Nodes[idx]
		}
	}
	return nil
}

func removeNodeById(nodeId int) {
	for i, n := range Nodes {
		if n.NodeId != me && n.NodeId == nodeId {
			Nodes = append(Nodes[:i], Nodes[i+1:]...)
		}
	}
	
}

func countNodesAlive() int {
	count := 0 
	for _, node := range Nodes {
		if node.NodeId != me {
			if node.Alive {
				count++
			}
		}
	}
	return count
}

func requestTimeout(n *Node) {
	fmt.Printf("\nEntering requestTimeout...")
	timer := time.NewTimer((time.Millisecond * time.Duration(RTT)) + time.Duration(CSSleepTime))
	<-timer.C
	fmt.Printf("\nNode #%v timed out. AwaitingReply is %v", n.NodeId, n.AwaitingReply)
	for n.AwaitingReply == true {
		n.IsThere = false
		sendAreYouThereMessage(*n)
		timer = time.NewTimer((time.Millisecond * time.Duration(RTT)) + time.Duration(CSSleepTime))
		<-timer.C
		if n.AwaitingReply == true && n.IsThere == false {
			fmt.Printf("\nNode #%v has died.", n.NodeId)
			n.Alive = false
			removeNodeById(n.NodeId)
			outstandingReplyCount = outstandingReplyCount - 1
			N--
			break
		}
	}
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

func run(isBootstrap bool) {
	conn, err = net.ListenPacket("udp", ipForListening)
	checkError(err)

	if isBootstrap {
		thisNode := Node{0, ipForListening, false, true, false, false}
		highestNodeId = 0
		Nodes = append(Nodes, thisNode)
		N = 0
		fmt.Printf("\nInitialized Nodes with thisNode: %+v", Nodes)
		go startListener()
		go invokeCS()
		// Testing findNode
		<-done
	} else {
		// Connect with Portal Node
		go startListener()
		go sendJoinMessage()
		timer := time.NewTimer(time.Millisecond * time.Duration(2 * RTT))
		<-timer.C
		if joinedNetwork == false {
			// Print something and exit
			fmt.Println("Portal node didn't respond in 2 RTTs. Exiting now...")
			os.Exit(1)
		}
		// We consider all nodes that are still not Alive to be actually dead
		// because they didn't reply to our NewNodeMessage. Those nodes will be deleted.
		for _, node := range Nodes {
			if node.NodeId != me && node.Alive == false {
				removeNodeById(node.NodeId)
			}
		}
		N = len(Nodes)
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

			outstandingReplyCount = N
			fmt.Printf("\nNode #%v trying to enter CS with sequence number %v", me, ourSequenceNumber)
			fmt.Printf("\nNODES ALIVE: %v", countNodesAlive())
			// Send REQUEST messages to all other nodes
			for i, node := range Nodes {
				if node.NodeId != me { //&& node.Alive == true {
					fmt.Printf("\nSending REQUEST to Node #%v at %v", node.NodeId, node.Address)
					Nodes[i].AwaitingReply = true
					go sendRequestMessage(node, ourSequenceNumber)
					go requestTimeout(&Nodes[i])
				}
			}
			// Wait for REPLY from all other nodes
			fmt.Printf("\nOutstanding Reply Count: %v\n", outstandingReplyCount)
			for outstandingReplyCount > 0 { 
				// fmt.Printf("%v", outstandingReplyCount)
			}
			// Enter CS
			fmt.Printf("\n\n------------------------")
			fmt.Printf("\nNODE #%v EXECUTING CS... [ %v ]", me, time.Now())
			fmt.Println("\n-------------------------")
			time.Sleep(time.Millisecond * time.Duration(CSSleepTime)) // CS
			fmt.Printf("\n\n------------------------")
			fmt.Printf("\nNODE #%v LEAVING CS ... [ %v ]", me, time.Now())
			fmt.Println("\n-------------------------")
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
		// fmt.Printf("\n\n%vReceived msg after Unmarshal: %+v\n", msgReceived)

		switch incomingMessage.Type {
    		case "JOIN":
				go handleJoinMessage(incomingMessage, conn)
			case "ACCEPT":
				go handleAcceptMessage(incomingMessage, conn)
			case "REQUEST":
				go handleRequestMessage(incomingMessage, conn)
			case "REPLY":
				go handleReplyMessage(incomingMessage, conn)
			case "NEW_NODE":
				go handleNewNodeMessage(incomingMessage, conn)
			case "CONFIRM_NEW_NODE":
				go handleNewNodeConfirmMessage(incomingMessage, conn)
			case "ARE_YOU_THERE":
				go handleAreYouThereMessage(incomingMessage, conn)
			case "I_AM_HERE":
				go handleIAmHereMessage(incomingMessage, conn)
		}
	}
	done <- 1
}

func sendAreYouThereMessage(node Node) {
	data := RequestData{}
	requestData, err := json.Marshal(&data)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "ARE_YOU_THERE",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: requestData,
	}

	areYouThereMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(areYouThereMessage, rAddr)
}

func handleAreYouThereMessage(areYouThereMessage Message, conn net.PacketConn) {
	fmt.Printf("\nReceived ARE_YOU_THERE from Node #%v", areYouThereMessage.SenderId)
	node := findNode(areYouThereMessage.SenderId)
	if node == nil {
		fmt.Printf("\nReceived a message from dead node (#%v).", areYouThereMessage.SenderId)
		return
	}

	if node.ReplyDeferred == false {
		sendReplyMessage(*node)
	} else {
		sendIAmHereMessage(*node)
	}
}

func sendIAmHereMessage(node Node) {
	data := RequestData{}
	requestData, err := json.Marshal(&data)
	if err != nil {
		fmt.Println(err)
	}

	msg := Message{
		Type: "I_AM_HERE",
		SenderId: me,
		SenderAddress: ipForListening, 
		Data: requestData,
	}

	iAmHereMessage, err := json.Marshal(&msg)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Printf("\n\nMarshalled data: %v\n", string(newNodeMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(iAmHereMessage, rAddr)
}

func handleIAmHereMessage(iAmHereMessage Message, conn net.PacketConn) {
	fmt.Printf("\nReceived I_AM_HERE message from Node #%v", iAmHereMessage.SenderId)
	node := findNode(iAmHereMessage.SenderId)
	if node == nil {
		fmt.Printf("\nReceived a message from dead node (#%v).", iAmHereMessage.SenderId)
		return
	}
	node.IsThere = true
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

func handleNewNodeMessage(newNodeMessage Message, conn net.PacketConn) {
	node := findNode(newNodeMessage.SenderId)
	if node != nil {
		fmt.Printf("\nReceived a newNodeMessage from a node that already exists. (#%v).", newNodeMessage.SenderId)
		return
	}

	newNode := Node{
		NodeId: newNodeMessage.SenderId,
		Address: newNodeMessage.SenderAddress,
		ReplyDeferred: false,
		Alive: true,
		AwaitingReply: false,
		IsThere: false,
	}
	Nodes = append(Nodes, newNode)
	// N = len(Nodes)
	go sendNewNodeConfirmMessage(newNode)
	N++
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

	// fmt.Printf("\n\nMarshalled data: %v\n", string(newNodeConfirmMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(newNodeConfirmMessage, rAddr)
}

func handleNewNodeConfirmMessage(newNodeConfirmMessage Message, conn net.PacketConn) {
	var receivedHighestSequenceNumber int
	err := json.Unmarshal(newNodeConfirmMessage.Data, &receivedHighestSequenceNumber)
	checkError(err)

	fmt.Printf("\nReceived NEW_NODE_CONFIRM from Node #%v with HighestSequenceNumber %v", newNodeConfirmMessage.SenderId, receivedHighestSequenceNumber)
	node := findNode(newNodeConfirmMessage.SenderId)
	if node == nil {
		fmt.Printf("\nReceived a NEW_NODE_CONFIRM message from dead Node (#%v).")
		return
	}
	// Node replied to newNodeMessage and will be considered Alive (see handleAcceptMessage)
	node.Alive = true
	if receivedHighestSequenceNumber >= highestSequenceNumber {
		highestSequenceNumber = receivedHighestSequenceNumber
	}
}

func sendRequestMessage (node Node, sequenceNumber int) {
	requestData := RequestData{sequenceNumber}
	
	data, err := json.Marshal(&requestData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("\n\n%vMarshalled JoingMessageData: %v\n", string(data))

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

	// fmt.Printf("\n\n%vMarshalled data: %v\n", string(requestMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(requestMessage, rAddr)
}

func handleRequestMessage(requestMessage Message, conn net.PacketConn) {
	var data RequestData
	err := json.Unmarshal(requestMessage.Data, &data)
	checkError(err)

	fmt.Printf("\nReceived REQUEST from Node #%v with SequenceNumber %v", requestMessage.SenderId, data.SequenceNumber)
	node := findNode(requestMessage.SenderId)
	if node == nil {
		fmt.Printf("\nReceived a message from dead node (#%v).", requestMessage.SenderId)
		return
	}

	if node.NodeId > highestNodeId {
		mutex.Lock()
		highestNodeId = node.NodeId
		mutex.Unlock()
	}

	// fmt.Printf("\nMy Node #%v and SequenceNumber %v", me, ourSequenceNumber)
	if data.SequenceNumber >= highestSequenceNumber {
		highestSequenceNumber = data.SequenceNumber	
	}
	mutex.Lock()
	deferRequest := false
	if requestingCS && ((data.SequenceNumber > ourSequenceNumber) || (data.SequenceNumber == ourSequenceNumber && requestMessage.SenderId > me)) {
		deferRequest = true
	}
	mutex.Unlock()

	if deferRequest == true {
		node.ReplyDeferred = true
	} else {
		go sendReplyMessage(*node)
	}
	// fmt.Printf("\nChanged ReplyDeferred of Node %+v to %+v", Nodes[idx], deferRequest)
	// fmt.Printf("\nNodes: %+v", Nodes)
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

	// fmt.Printf("\n\n%vMarshalled data: %v\n", string(replyMessage))
	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	conn.WriteTo(replyMessage, rAddr)
}

func handleReplyMessage(replyMessage Message, conn net.PacketConn) {
	fmt.Printf("\nReceived REPLY from Node #%v", replyMessage.SenderId)
	node := findNode(replyMessage.SenderId)
	if node == nil {
		fmt.Printf("\nReceived a message from dead node (#%v).", replyMessage.SenderId)
		return
	}
	node.AwaitingReply = false
	outstandingReplyCount = outstandingReplyCount - 1

}

func sendJoinMessage() {
	JoinMessageData := JoinData{ipForListening}
	
	data, err := json.Marshal(&JoinMessageData)
	if err != nil {
		fmt.Println(err)
	}
	// fmt.Printf("\n\n%vMarshalled JoingMessageData: %v\n", string(data))

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

func handleJoinMessage(joinMessage Message, conn net.PacketConn) {
	newNodeIP := joinMessage.SenderAddress
	
	// Determine NodeId for newNode
	// TODO this has to be mutually shared between all nodes.
	mutex.Lock()
	highestNodeId++
	newNodeId := highestNodeId
	mutex.Unlock()

	tempNewNode := Node{
		NodeId: newNodeId, 
		Address: newNodeIP, 
		ReplyDeferred: false,
	}

	fmt.Printf("\n\nNODE REQUESTING TO JOIN: %+v", tempNewNode)

	go sendAcceptMessage(tempNewNode, newNodeId)
}

func sendAcceptMessage(node Node, newNodeId int) {
	acceptData := AcceptData{
		AssignedNodeId: newNodeId,
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

	rAddr, err := net.ResolveUDPAddr("udp", node.Address)
	checkError(err)
	conn.WriteTo(acceptMessageJson, rAddr)
}

func handleAcceptMessage(acceptMessage Message, conn net.PacketConn) {
	var data AcceptData
	err := json.Unmarshal(acceptMessage.Data, &data)
	checkError(err)

	Nodes = data.ActiveNodes
	me = data.AssignedNodeId
	if me > highestNodeId {
		mutex.Lock()
		highestNodeId = me
		mutex.Unlock()
	}
	// N = len(Nodes)
	highestSequenceNumber = data.HighestSequenceNumber
	for i, node := range Nodes {
		if node.NodeId != me {
			// All other nodes are considered dead until they reply 
			// to newNodeMessage with newNodeConfirmMessage
		Nodes[i].Alive = false 
		go sendNewNodeMessage(node)	
		}
	}
	// timer := time.NewTimer(time.Millisecond * time.Duration(RTT))
	// <-timer.C
	// We wait RTT and consider all nodes that are still not Alive to be actually dead
	// because they didn't reply to our NewNodeMessage. Those nodes will be deleted.
	// for _, node := range Nodes {
	// 	if node.NodeId != me && node.Alive == false {
	// 		removeNodeById(node.NodeId)
	// 	}
	// }
	fmt.Printf("\n\nFinal status of Joining Node\nNumber of nodes: %+v; \nNodes: %+v; \nI am Node #%v and highestSequenceNumber %v", N, Nodes, me, highestSequenceNumber)
	joinedNetwork = true
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}