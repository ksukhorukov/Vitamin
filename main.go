package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"unicode"
	"strings"
)

var storage = make(map[string]string)

var set_regexp = regexp.MustCompile(`^set\((\w+),(\w+),(\d+)\)$`)
var get_regexp = regexp.MustCompile(`^get\((\w+)\)$`)

const SET_INSTRUCTION = "set"
const GET_INSTRUCTION = "get"

const ERROR_UNRECOGNIZED_COMMAND = "Unrecognized command"

type Command struct {
	instruction string
	key         string
	value       string
	creation    int32
	ttl         int32
}

func main() {
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		msg := input.Text()
		command, error := parseCommand(msg)
		if error != nil {
			fmt.Printf("Error: %s\n", error)
		} else {
			executeCommand(command)
		}
	}
}

func parseCommand(msg string) (Command, error) {
	// set(key, value, ttl)
	// get(key)

	var cmd Command
	var err error

	msg = stripSpaces(msg)

	matched_set := set_regexp.Match([]byte(msg))
	matched_get := get_regexp.Match([]byte(msg))

	if matched_set {
		formCommand(msg, SET_INSTRUCTION, &cmd, set_regexp)
	} else if matched_get {
		formCommand(msg, GET_INSTRUCTION, &cmd, get_regexp)
	} else {
		err = fmt.Errorf("%s", ERROR_UNRECOGNIZED_COMMAND)
	}

	return cmd, err
}

func formCommand(msg string, instruction string, cmd *Command, pattern *regexp.Regexp) {
	match := pattern.FindStringSubmatch(msg)

	if instruction == SET_INSTRUCTION {
		cmd.key = match[0]
		cmd.value = match[1]
	} else if instruction == GET_INSTRUCTION {
		cmd.key = match[0]
	}

	cmd.instruction = instruction

}

func stripSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func executeCommand(cmd Command) {
	fmt.Println("executing command")
}
