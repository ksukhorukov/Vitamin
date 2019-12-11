package main

import (
	"bufio"
	"fmt"
	"time"
	"os"
	"regexp"
	"unicode"
	"strings"
	"strconv"
)

var storage = make(map[string]Record)

var set_regexp = regexp.MustCompile(`set\(\"(.*)\",\"(.*)\",(\d+)\)$`)
var get_regexp = regexp.MustCompile(`^get\(\"(.*)\"\)$`)

const SET_INSTRUCTION = "set"
const GET_INSTRUCTION = "get"

const ERROR_UNRECOGNIZED_COMMAND = "Unrecognized command"
const ERROR_NO_RECORD_FOUND = "No record found"
const ERROR_RECORD_EXPIRED = "Record expired"
const ERROR_COMMAND_RUNTIME = "Command execution runtime error"
const ERROR_TTL_NON_INTEGER = "TTL is not an integer"

type Command struct {
	instruction string
	key         string
	value       string
	ttl         int64
}

type Record struct {
	value string
	timestamp int64
	ttl		int64
}

func main() {
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		msg := input.Text()
		command, error := parseCommand(msg)
		if error != nil {
			fmt.Printf("Error: %s\n", error)
		} else {
			result, error := executeCommand(&command)

			if error != nil {
				fmt.Printf("Error: %s\n", error)
			} else {
				fmt.Printf("%s\n", result)
			}
		}
	}
}

func executeCommand(cmd *Command) (string, error) {
	if cmd.instruction == SET_INSTRUCTION {
		return set(cmd)
	} else if cmd.instruction == GET_INSTRUCTION {
		return get(cmd)
	}

	return cmd.instruction, fmt.Errorf("%s", ERROR_COMMAND_RUNTIME)
}

func set(cmd *Command) (string, error) {
	var record Record 
	var err error

	record.value = cmd.value
	record.timestamp = time.Now().Unix()
	record.ttl = int64(cmd.ttl)

	storage[cmd.key] = record

	// fmt.Printf("set command. %s = %s\n", cmd.key, storage[cmd.key].value)

	return record.value, err
}

func get(cmd *Command) (string, error) {
	var record Record 
	var err error 

	// fmt.Printf("key: %s\n", cmd.key)
	// fmt.Printf("value: %s\n", storage[cmd.key].value)

	record, ok := storage[cmd.key]

	if !ok {
		return cmd.key, fmt.Errorf("%s", ERROR_NO_RECORD_FOUND)
	}
	
	// fmt.Printf("timestamp: %d. ttl: %d.\n", record.timestamp, record.ttl)

	if (record.ttl + record.timestamp) < time.Now().Unix() {
		return cmd.key, fmt.Errorf("%s", ERROR_RECORD_EXPIRED)
	}

	return record.value, err
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

	if instruction == SET_INSTRUCTION {
		cmd.key = strings.TrimSpace(match[1])
		cmd.value = strings.TrimSpace(match[2])
		cmd.ttl, err = strconv.ParseInt(strings.TrimSpace(match[3]), 10, 64)

		if err != nil {
			return fmt.Errorf("%s", ERROR_TTL_NON_INTEGER)
		}
		// fmt.Printf("form command. %s. %s = %s\n", cmd.instruction, cmd.key, cmd.value)
	} else if instruction == GET_INSTRUCTION {
		cmd.key = match[1]
		// fmt.Printf("form command. %s. %s\n", cmd.instruction, cmd.key)
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