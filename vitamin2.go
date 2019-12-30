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
	"bytes"
	"io"
)

var storage = make(map[[HASH_SIZE]byte]Record)
var mcounter int64

var peers = []string{ "127.0.0.1:8080" }
var peers_connection = make(map[string]net.Conn)

var set_regexp = regexp.MustCompile(`set\(\"(.*)\",\"(.*)\",(\d+)\)$`)
var get_regexp = regexp.MustCompile(`^get\(\"(.*)\"\)$`)
var network_get_regexp = regexp.MustCompile(`^network_get\(\"(.*)\",\"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\:\d{1,5})\"\)$`)
var network_get_success_regexp = regexp.MustCompile(`(.*) (.*) (.*) (.*)`)

const SET_INSTRUCTION = "set"
const GET_INSTRUCTION = "get"
const NETWORK_GET_INSTRUCTION = "network_get"

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
	socket      string
	key         [HASH_SIZE]byte
	value       string
	timestamp int64
	ttl         int64
}

type Record struct {
	value     string
	size      int64
	timestamp int64
	ttl       int64
}

func main() {
	go garbageCollector()
	go connectPeers()

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
		command, error := parseCommand(msg)
		if error != nil {
			// fmt.Printf("Error: %s\n", error)
			fmt.Fprintf(c, "Error: %s\n", error)
		} else {
			result, error := executeCommand(&command)

			if error != nil {
				// fmt.Printf("Error: %s\n", error)
				fmt.Fprintf(c, "Error: %s\n", error)
			} else {
				// fmt.Printf("%s\n", result)
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

	if cmd.instruction == SET_INSTRUCTION {
		result, err = set(cmd)
	} else if cmd.instruction == GET_INSTRUCTION {
		result, err = get(cmd)
	} else if cmd.instruction == NETWORK_GET_INSTRUCTION {
		networkGet(cmd)
	}

	return result, err
}

func connectPeers() {
	for _, socket := range(peers) {
		for i := 0; i < PEERS_CONNECTION_RETRIES; i++ {
			conn, err := net.Dial("tcp", socket)

			if err != nil {
				log.Println(err)
				time.Sleep(PEERS_CONNECTION_WAITING_TIME)
			} else {
				fmt.Println("Connected to %s", conn.RemoteAddr().String())
				peers_connection[socket] = conn
				break
			}
		}
	}
}

func networkGet(cmd *Command) {
	var record Record

	// fmt.Printf("key: %s\n", cmd.key)
	// fmt.Printf("value: %s\n", storage[cmd.key].value)

	record, ok := storage[cmd.key]

	if !ok {
		fmt.Fprintf(peers_connection[cmd.socket], "%s", ERROR_RECORD_NOT_FOUND)
	} else {
		fmt.Fprintf(peers_connection[cmd.socket], "%s %s %s %s", record.value, record.size, record.timestamp, record.ttl)
	}
}

func broadcastGet(cmd *Command) (Record, error) {
	var record Record
	var records []Record
	var err error
	var result Record
	var data string
	var response bytes.Buffer

	for _, connection := range(peers_connection) {
		fmt.Fprintf(connection, "%s(\"%s\",\"%s\",\"%s\",\"%s\")", NETWORK_GET_INSTRUCTION, cmd.key, cmd.socket, cmd.timestamp, cmd.ttl)
		io.Copy(&response, connection)


		data = fmt.Sprintf("%x", response)
		get_response_match := network_get_success_regexp.Match([]byte(data))

		if get_response_match {
			get_response_data := network_get_success_regexp.FindStringSubmatch(data)
			value := get_response_data[1]
			size, _ := strconv.ParseInt(get_response_data[2], 10, 64)
			timestamp, _ := strconv.ParseInt(get_response_data[3], 10, 64)
			ttl, _ := strconv.ParseInt(get_response_data[4], 10, 64)

			record.value = value
			record.size = size
			record.timestamp = timestamp
			record.ttl = ttl

			records = append(records, record)
		}
	}

	result, err = findLatestRecord(records)

	return result, err
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
		key = fmt.Sprintf("%x", cmd.key)
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

	// fmt.Printf("set command. %s = %s\n", cmd.key, storage[cmd.key].value)

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

		record, err = broadcastGet(cmd)
		
		if err != nil {
			return fmt.Sprintf("%x", cmd.key), fmt.Errorf("%s", ERROR_RECORD_NOT_FOUND)		
		}
	}
	
	// fmt.Printf("timestamp: %d. ttl: %d.\n", record.timestamp, record.ttl)

	if recordExpired(record) {
		// cache invalidation. step 1.
		deleteKeyFromStorage(cmd.key)

		mcounter -= record.size

		return fmt.Sprintf("%x", cmd.key), fmt.Errorf("%s", ERROR_RECORD_EXPIRED)
	}

	return record.value, err
}

func deleteKeyFromStorage(key [HASH_SIZE]byte) {
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

	if matched_set {
		err = formCommand(msg, SET_INSTRUCTION, &cmd, set_regexp)
	} else if matched_get {
		err = formCommand(msg, GET_INSTRUCTION, &cmd, get_regexp)
	} else if matched_netowork_get {
		err = formCommand(msg, NETWORK_GET_INSTRUCTION, &cmd, network_get_regexp)
	} else {
		err = fmt.Errorf("%s", ERROR_UNRECOGNIZED_COMMAND)
	}

	return cmd, err
}

func formCommand(msg string, instruction string, cmd *Command, pattern *regexp.Regexp) error {
	var err error

	match := pattern.FindStringSubmatch(msg)
	cmd.instruction = instruction

	if instruction != NETWORK_GET_INSTRUCTION {
		cmd.key = sha256.Sum256([]byte(strings.TrimSpace(match[1])))	
	} else if instruction == NETWORK_GET_INSTRUCTION {
		//cmd.key = [32]byte(match[1])
		copy(cmd.key[:], match[1])
		cmd.socket = match[2]
		cmd.timestamp, _ = strconv.ParseInt(match[3], 10, 64)
		cmd.ttl, _ = strconv.ParseInt(match[4], 10, 64)
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
