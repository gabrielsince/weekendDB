package main

import (
	enums "2/enums"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func do_meta_command(input string) int {
	if strings.HasPrefix(input, ".exit") {
		os.Exit(enums.MetaCommandResult.META_COMMAND_SUCCESS)
	}

	return enums.MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepare_statement(input string, statement enums.Statement) int {

	if strings.HasPrefix(input, "insert") {
		statement.Typ = enums.StatementType.STATEMENT_INSERT
		return enums.PrepareResult.PREPARE_SUCCESS
	}

	if strings.HasPrefix(input, "select") {
		statement.Typ = enums.StatementType.STATEMENT_SELECT
		return enums.PrepareResult.PREPARE_SUCCESS
	}

	return enums.PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_statement(statement enums.Statement) {
	switch statement.Typ {
	case enums.StatementType.STATEMENT_INSERT:
		fmt.Println("This is where we would do an insert.")
	case enums.StatementType.STATEMENT_SELECT:
		fmt.Println("This is where we would do an insert.")
	}
}

func main() {
	fmt.Println("Begin of the sql terminal!")
	inputReader := bufio.NewReader(os.Stdin)
loop:
	for {
		fmt.Print("db: >")
		input, err := inputReader.ReadString('\n')
		if err != nil {
			fmt.Println("There were errors reading, exiting program.")
			return
		}
		if strings.HasPrefix(input, ".") {
			switch do_meta_command(input) {
			case enums.MetaCommandResult.META_COMMAND_SUCCESS:
				continue loop
			case enums.MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s' \n", input)
				continue loop
			}
		}

		statement := enums.Statement{}
		switch prepare_statement(input, statement) {
		case enums.PrepareResult.PREPARE_SUCCESS:
			fmt.Printf("Success input SQL '%s'.\n", input)
			break loop
		case enums.PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue loop
		}
		execute_statement(statement)

		fmt.Printf(" sql: %s.", input)
	}
	fmt.Println("End of the sql terminal!")
}
