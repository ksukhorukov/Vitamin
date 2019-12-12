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

var storage = make(map[[HASH_SIZE]byte]Record)
var mcounter int64

var set_regexp = regexp.MustCompile(`set\(\"(.*)\",\"(.*)\",(\d+)\)$`)
var get_regexp = regexp.MustCompile(`^get\(\"(.*)\"\)$`)

const SET_INSTRUCTION = "set"
const GET_INSTRUCTION = "get"

const HASH_SIZE = sha256.Size

const MEMORY_LIMIT = 1024 * 1024 //1kb
const GC_WAITING_TIME = 1 * time.Second
const SERVER_ADDRESS = "127.0.0.1"
const SERVER_PORT = 8080

const ERROR_UNRECOGNIZED_COMMAND = "Unrecognized command"
const ERROR_NO_RECORD_FOUND = "No record found"
const ERROR_RECORD_EXPIRED = "Record expired"
const ERROR_COMMAND_RUNTIME = "Command execution runtime error"
const ERROR_TTL_NON_INTEGER = "TTL is not an integer"
const ERROR_MEMORY_OVERUSAGE = "Memory overusage"

type Command struct {
	instruction string
	key         [HASH_SIZE]byte
	value       string
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

	listener, err := net.Listen("tcp", SERVER_ADDRESS+":"+strconv.Itoa(SERVER_PORT))

	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConn(conn)
	}

}

func handleConn(c net.Conn) {
	defer c.Close()

	input := bufio.NewScanner(c)
	for input.Scan() {
		msg := input.Text()
		command, error := parseCommand(msg)
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
					delete_key_from_storage(key)
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
		return fmt.Sprintf("%x", cmd.key), fmt.Errorf("%s", ERROR_NO_RECORD_FOUND)
	}

	// fmt.Printf("timestamp: %d. ttl: %d.\n", record.timestamp, record.ttl)

	if recordExpired(record) {
		// cache invalidation. step 1.
		delete_key_from_storage(cmd.key)

		mcounter -= record.size

		return fmt.Sprintf("%x", cmd.key), fmt.Errorf("%s", ERROR_RECORD_EXPIRED)
	}

	return record.value, err
}

func delete_key_from_storage(key [HASH_SIZE]byte) {
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

	if matched_set {
		err = formCommand(msg, SET_INSTRUCTION, &cmd, set_regexp)
	} else if matched_get {
		err = formCommand(msg, GET_INSTRUCTION, &cmd, get_regexp)
	} else {
		err = fmt.Errorf("%s", ERROR_UNRECOGNIZED_COMMAND)
	}

	return cmd, err
}

func formCommand(msg string, instruction string, cmd *Command, pattern *regexp.Regexp) error {
	var err error

	match := pattern.FindStringSubmatch(msg)
	cmd.instruction = instruction
	cmd.key = sha256.Sum256([]byte(strings.TrimSpace(match[1])))

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
