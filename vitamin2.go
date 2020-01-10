package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var storage = make(map[string]Record)
var mcounter int64

var peers = []string{ "127.0.0.1:8080" }

var set_regexp = regexp.MustCompile(`set\(\"(.*)\",\"(.*)\",(\d+)\)$`)
var get_regexp = regexp.MustCompile(`^get\(\"(.*)\"\)$`)
var network_get_regexp = regexp.MustCompile(`^network_get\(\"(\w+)\"\)$`)
var network_get_success_response_regexp = regexp.MustCompile(`^network_get_response\(\"(\w+)\",\"(\w+)\",(\d+),(\d+),(\d+)\)$`)
var response_error_regexp =  regexp.MustCompile(`^Error: (.+)$`)

const SET_INSTRUCTION = "set"
const GET_INSTRUCTION = "get"
const NETWORK_GET_INSTRUCTION = "network_get"
const NETWORK_GET_SUCCESS_RESPONSE = "network_get_response"

const HASH_SIZE = sha256.Size

const MEMORY_LIMIT = 1024 * 1024 //1kb
const GC_WAITING_TIME = 1 * time.Second
const PEERS_CONNECTION_WAITING_TIME = 5 * time.Second
const PEERS_CONNECTION_RETRIES = 5
const SERVER_ADDRESS = "127.0.0.1"
const SERVER_PORT = 8081

const ERROR_UNRECOGNIZED_COMMAND = "Unrecognized command"
const ERROR_RECORD_NOT_FOUND = "Record not found"
const ERROR_RECORD_EXPIRED = "Record expired"
const ERROR_COMMAND_RUNTIME = "Command execution runtime error"
const ERROR_TTL_NON_INTEGER = "TTL is not an integer"
const ERROR_MEMORY_OVERUSAGE = "Memory overusage"

type Command struct {
	instruction string
	key         string
	value       string
	timestamp int64
	ttl         int64
	size 				int64
}

type Record struct {
	value     string
	size      int64
	timestamp int64
	ttl       int64
}

func main() {
	go garbageCollector()

	listener, err := net.Listen("tcp", socketAddress(SERVER_ADDRESS, SERVER_PORT))

	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		go handleConn(conn)
	}

}

func socketAddress(host string, port int) string {
	return SERVER_ADDRESS + ":" + strconv.Itoa(SERVER_PORT)
}

func handleConn(c net.Conn) {
	defer c.Close()

	input := bufio.NewScanner(c)
	for input.Scan() {
		msg := input.Text()
		fmt.Printf("Message: %s\n", msg)
		command, error := parseCommand(msg)
		fmt.Printf("Command parsed.")
		if error != nil {
			fmt.Printf("Error: %s\n", error)
			fmt.Fprintf(c, "Error: %s\n", error)
		} else {
			result, error := executeCommand(&command)

			if error != nil {
				fmt.Printf("Error: %s\n", error)
				fmt.Fprintf(c, "Error: %s\n", error)
			} else {
				fmt.Printf("%s\n", result)
				fmt.Fprintf(c, "%s\n", result)
			}
		}
	}
}

func garbageCollector() {
	paretto := float64(MEMORY_LIMIT * 0.8)

	for {
		if mcounter >= int64(paretto) {
			// collect garbage
			// fmt.Printf("GC. memory usage: %d\n", mcounter)
			for key, record := range storage {
				if recordExpired(record) {
					deleteKeyFromStorage(key)
					mcounter -= record.size
				}
			}
			// fmt.Printf("GC. cleaned memory: %d\n", mcounter)
		}

		time.Sleep(GC_WAITING_TIME)
	}
}

func recordExpired(record Record) bool {
	return (record.ttl + record.timestamp) < time.Now().Unix()
}

func executeCommand(cmd *Command) (string, error) {
	var result string
	var err error

	fmt.Printf("executing command %s\n", cmd.instruction)
	if cmd.instruction == SET_INSTRUCTION {
		result, err = set(cmd)
	} else if cmd.instruction == GET_INSTRUCTION {
		result, err = get(cmd)
	} else if cmd.instruction == NETWORK_GET_INSTRUCTION {
		fmt.Printf("network get instruction received\n")
		result, err = networkGet(cmd)
	} else if cmd.instruction == NETWORK_GET_SUCCESS_RESPONSE {
		result, err = networkGetResponse(cmd)
	}

	return result, err
}

func connectPeer(socket string) (net.Conn, error) {
	conn, err := net.Dial("tcp", socket)

	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("Connected to ", conn.RemoteAddr().String())
	}

	return conn, err
}

func networkGetResponse(cmd *Command)  (string, error) {
	var err error
	var record Record

	fmt.Printf("networkGetResponse function.\n")
	fmt.Printf("command: %s %s %d %d %d\n", cmd.key, cmd.value, cmd.size, cmd.timestamp, cmd.ttl)

	record.value = cmd.value
	record.size = cmd.size 
	record.timestamp = cmd.timestamp
	record.ttl = cmd.ttl

	if recordExpired(record) {
		fmt.Printf("recordExpired. netwoekGetResponse\n")
		mcounter -= record.size
		err = fmt.Errorf("%s", ERROR_RECORD_EXPIRED)
	} else {
		storage[cmd.key] = record
	}

	return record.value, err
}

func networkGet(cmd *Command) (string, error) {
	var result string
	var err error

	fmt.Printf("network get received\n")
	fmt.Printf("key: %s\n", cmd.key)
	fmt.Printf("value: %s\n", storage[cmd.key].value)

	record, ok := storage[cmd.key]

	if ok {
		fmt.Printf("networkGet. Record found\n")
		fmt.Printf("Record: %s %s %d %d %d", cmd.key, record.value, record.size, record.timestamp, record.ttl)
		result = fmt.Sprintf("%s(\"%s\",\"%s\",%d,%d,%d)\n", NETWORK_GET_SUCCESS_RESPONSE, cmd.key, record.value, record.size, record.timestamp, record.ttl)		
	} else {
		fmt.Printf("networkGet. Error: record not found\n")
		err = fmt.Errorf("%s", ERROR_RECORD_NOT_FOUND)
	}

	return result, err
}

func broadcastGet(cmd *Command) (string, error) {
	var result string
	var error error
	var connection net.Conn
	var broadcast_get_response_cmd Command

	fmt.Printf("%s\n","Starting broadcast")
	for _, socket := range(peers) {
		connection, error = connectPeer(socket)

		if error != nil {
			log.Println(error)
			continue
		}

		fmt.Printf("broadcast get. key = %s\n", cmd.key)
		fmt.Fprintf(connection, "%s(\"%s\")\n", NETWORK_GET_INSTRUCTION, cmd.key)
		fmt.Printf("network_get instruciton\n")

		result, error = bufio.NewReader(connection).ReadString('\n')

		connection.Close()

		if error != nil {
			log.Println(error)
		} else {
			broadcast_get_response_cmd, error = parseCommand(result)
			if error != nil {
				log.Println(error)
			} else {
				fmt.Printf("broadcastGet. command parsed. executing %s\n", broadcast_get_response_cmd.instruction)
				return executeCommand(&broadcast_get_response_cmd)
			}
		}

		result, error = "", nil
	}
	
	return result, fmt.Errorf(ERROR_RECORD_NOT_FOUND)
}

func findLatestRecord(records []Record) (Record, error) {
	var result Record
	var err error

	if len(records) == 0 {
		return result, fmt.Errorf("%s", ERROR_RECORD_NOT_FOUND)		
	}

	result = records[0]

	for _, record := range(records) {
		if record.timestamp >  result.timestamp {
			result = record
		}
	}

	return result, err
}

func set(cmd *Command) (string, error) {
	var record Record
	var key string
	var err error
	var size int64

	size = int64(len(cmd.value) + len(cmd.key))

	if memoryOveruse(mcounter + size) {
		key = cmd.key
		err = fmt.Errorf("%s", ERROR_MEMORY_OVERUSAGE)
		return key, err
	} else {
		mcounter += size
	}

	record.value = cmd.value
	record.timestamp = time.Now().Unix()
	record.ttl = int64(cmd.ttl)
	record.size = size

	storage[cmd.key] = record

	fmt.Printf("set command. %s = %s\n", cmd.key, storage[cmd.key].value)

	return record.value, err
}

func memoryOveruse(size int64) bool {
	return size > MEMORY_LIMIT
}

func get(cmd *Command) (string, error) {
	var record Record
	var err error

	// fmt.Printf("key: %s\n", cmd.key)
	// fmt.Printf("value: %s\n", storage[cmd.key].value)

	record, ok := storage[cmd.key]

	if !ok {
		fmt.Printf("%s\n", "No key in storage")
		fmt.Printf("%s\n", "Starting broadcast")
		return broadcastGet(cmd)
	}
		
	if recordExpired(record) {
		// cache invalidation. step 1.
		fmt.Printf("recordExpired. Get request\n")
		deleteKeyFromStorage(cmd.key)

		mcounter -= record.size

		return cmd.key, fmt.Errorf("%s\n", ERROR_RECORD_EXPIRED)
	}

	return record.value, err
}

func deleteKeyFromStorage(key string) {
	delete(storage, key)
}

func parseCommand(msg string) (Command, error) {
	// set(key, "value", ttl)
	// get(key)

	var cmd Command
	var err error

	msg = strings.TrimSpace(msg)

	matched_set := set_regexp.Match([]byte(msg))
	matched_get := get_regexp.Match([]byte(msg))
	matched_netowork_get := network_get_regexp.Match([]byte(msg))
	matched_network_get_response := network_get_success_response_regexp.Match([]byte(msg))
	matched_response_error := response_error_regexp.Match([]byte(msg))

	fmt.Printf("parse command. message: %s\n", msg)

	if matched_set {
		err = formCommand(msg, SET_INSTRUCTION, &cmd, set_regexp)
	} else if matched_get {
		err = formCommand(msg, GET_INSTRUCTION, &cmd, get_regexp)
	} else if matched_netowork_get {
		fmt.Printf("matched_netowork_get\n")
		err = formCommand(msg, NETWORK_GET_INSTRUCTION, &cmd, network_get_regexp)
	} else if matched_network_get_response {
		err = formCommand(msg, NETWORK_GET_SUCCESS_RESPONSE, &cmd, network_get_success_response_regexp)
		fmt.Printf("matched network get success response")
	} else if matched_response_error {
		fmt.Printf("matched response error")
		err = fmt.Errorf(msg)
	} else {
		err = fmt.Errorf("%s", ERROR_UNRECOGNIZED_COMMAND)
	}

	return cmd, err
}

func formCommand(msg string, instruction string, cmd *Command, pattern *regexp.Regexp) error {
	var err error

	match := pattern.FindStringSubmatch(msg)
	cmd.instruction = instruction

	fmt.Printf("form command. %s\n", cmd.instruction)

	if instruction != NETWORK_GET_INSTRUCTION  && instruction != NETWORK_GET_SUCCESS_RESPONSE {
		cmd.key = fmt.Sprintf("%x",sha256.Sum256([]byte(strings.TrimSpace(match[1]))))
	} else if instruction == NETWORK_GET_INSTRUCTION {
		//cmd.key = [32]byte(match[1])
		fmt.Printf("form command. network_get instruction\n")
		cmd.key = match[1]
		fmt.Printf("cmd key %s.\n", cmd.key)
	} else if instruction == NETWORK_GET_SUCCESS_RESPONSE {
		fmt.Printf("form command. network_get_success_response\n")
		cmd.key = match[1]
		cmd.value = match[2]
		cmd.size, err =   strconv.ParseInt(match[3], 10, 64)
		cmd.timestamp, err = strconv.ParseInt(match[4], 10, 64)
		cmd.ttl, err = strconv.ParseInt(match[5], 10, 64)

	}
	
	if instruction == SET_INSTRUCTION {
		cmd.value = strings.TrimSpace(match[2])
		cmd.ttl, err = strconv.ParseInt(strings.TrimSpace(match[3]), 10, 64)

		if err != nil {
			return fmt.Errorf("%s", ERROR_TTL_NON_INTEGER)
		}
		// fmt.Printf("form command. %s. %s = %s\n", cmd.instruction, cmd.key, cmd.value)
	}

	return err
}

func stripSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}
