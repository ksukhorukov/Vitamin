package main

import (
	"strings"
	"regexp"
	"bufio"
	"fmt"
	"os"
)

var storage = make(map[string]string)

var set_regexp = regexp.MustCompile(`^set\((\w+),(\w+)\)$`)
var get_regexp = regexp.MustCompile(`^get\((\w+)\)$`)

type Command struct {
	instruction string 
	payload string 
	creation int32
	ttl int32
}

func main() {
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		msg := input.Text()
		command  := parseCommand(msg)
		executeCommand(command)
	}
}


func parseCommand(msg string) Command {
	// set(key, value, ttl)
	// get(key)

	msg = strings.TrimSpace(msg)

	fmt.Println(msg)

	matched_get := get_regexp.Match([]byte(msg))
	matched_set := set_regexp.Match([]byte(msg))

	if matched_get {
		fmt.Println("matched get")
	} else if matched_set {
		fmt.Println("matched set")
	} else {
		fmt.Println("unrecognized command")
	}

	var command Command

	return command
}

func executeCommand(cmd Command) {
	fmt.Println("executing command")
}