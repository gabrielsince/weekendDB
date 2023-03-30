package main

import (
	enums "3/enums"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

func print_row(row *enums.Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}

/**
*  切片类似于netty的缓存切片
*
*  切片和结构体内存不共用
*  binary.Write
*  切片和结构体内存共用
*  unsafe.Pointer
*
**/
func do_meta_command(input string) int {
	if strings.HasPrefix(input, ".exit") {
		os.Exit(enums.MetaCommandResult.META_COMMAND_SUCCESS)
	}

	return enums.MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepare_statement(input string, statement *enums.Statement) int {

	if strings.HasPrefix(input, "insert") {
		fmt.Println(input)
		var username, email string
		var id int8
		statement.Typ = enums.StatementType.STATEMENT_INSERT
		args_assigned, err := fmt.Sscanf(input, "insert %d %s %s", &id,
			&username, &email)
		if err != nil {
			fmt.Printf("There were errors in insert syntax.%s \n", err)
			return enums.PrepareResult.PREPARE_SYNTAX_ERROR
		}

		statement.Row_to_insert.Id = id
		copy(statement.Row_to_insert.Username[:32], username)
		copy(statement.Row_to_insert.Email[:256], email)
		// statement.Row_to_insert.Username = values
		// statement.Row_to_insert.Email = email

		if args_assigned < 3 {
			fmt.Println("There were errors in insert syntax args assgined is less than 3.")
			return enums.PrepareResult.PREPARE_SYNTAX_ERROR
		}

		return enums.PrepareResult.PREPARE_SUCCESS
	}

	if strings.HasPrefix(input, "select") {
		statement.Typ = enums.StatementType.STATEMENT_SELECT
		return enums.PrepareResult.PREPARE_SUCCESS
	}

	return enums.PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_insert(statement enums.Statement, table *Table) int {
	if table.num_rows >= TABLE_MAX_ROWS {
		return enums.ExecuteResult.EXECUTE_TABLE_FULL
	}
	row_to_insert := &(statement.Row_to_insert)
	page_num, offset := row_slot_tuple(table, table.num_rows)

	b := table.pages[page_num][offset:]
	serialize_row(row_to_insert, &b)

	// serialize_row(row_to_insert, row_slot(table, table.num_rows))
	table.num_rows += 1
	return enums.ExecuteResult.EXECUTE_SUCCESS
}

func execute_select(statement enums.Statement, table *Table) int {
	var row enums.Row
	for i := 0; i < table.num_rows; i++ {
		deserialize_row(row_slot(table, i), &row)
		print_row(&row)
	}
	return enums.ExecuteResult.EXECUTE_SUCCESS
}

func execute_statement(statement enums.Statement, table *Table) int {
	switch statement.Typ {
	case enums.StatementType.STATEMENT_INSERT:
		fmt.Println("This is where we would do an insert.")
		return execute_insert(statement, table)
	case enums.StatementType.STATEMENT_SELECT:
		fmt.Println("This is where we would do an select.")
		return execute_select(statement, table)
	}
	return 0
}

// const uint32_t ID_SIZE = size_of_attribute(Row, id);
// const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
// const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
// const uint32_t ID_OFFSET = 0;
// const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
// const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
// const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const (
	ID_SIZE         = 8
	USERNAME_SIZE   = 32
	EMAIL_SIZE      = 256
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_OFFSET
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

func serialize_row(source *enums.Row, destination *[]byte) {

	buf := &bytes.Buffer{}
	//buf := bytes.NewBuffer(destination)
	err := binary.Write(buf, binary.LittleEndian, source)
	if err != nil {
		panic(err)
	}
	buf.Bytes()
	copy(*destination, buf.Bytes())
	// memcpy(destination + ID_OFFSET, &(source.id), ID_SIZE)
	// memcpy(destination + USERNAME_OFFSET, &(source.username), USERNAME_SIZE)
	// memcpy(destination + EMAIL_OFFSET, &(source.email), EMAIL_SIZE)
}

func deserialize_row(source []byte, destination *enums.Row) {

	buf := bytes.NewBuffer(source)
	err := binary.Read(buf, binary.LittleEndian, destination)
	if err != nil {
		panic(err)
	}

	// memcpy(&(destination.id), source + ID_OFFSET, ID_SIZE)
	// memcpy(&(destination.username), source + USERNAME_OFFSET, USERNAME_SIZE)
	// memcpy(&(destination.email), source + EMAIL_OFFSET, EMAIL_SIZE)
}

const (
	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Table struct {
	num_rows int
	pages    [TABLE_MAX_PAGES][PAGE_SIZE]byte
}

func row_slot(table *Table, row_num int) []byte {
	page_num := row_num / ROWS_PER_PAGE
	page := table.pages[page_num]
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	return page[byte_offset:]
}

func row_slot_tuple(table *Table, row_num int) (int, int) {
	page_num := row_num / ROWS_PER_PAGE
	// page := table.pages[page_num]
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	return page_num, byte_offset
}

func new_table() Table {
	fmt.Println("malloc the table!")
	return Table{}
}

func free_table(table Table) {
	fmt.Println("free the table!")
}

func main() {
	fmt.Println("Begin of the sql terminal!")
	table := new_table()
	inputReader := bufio.NewReader(os.Stdin)
loop:
	for {
		fmt.Print("db: >")
		input, err := inputReader.ReadString('\n')
		if err != nil {
			fmt.Println("There were errors reading, exiting program.")
			return
		}
		// input := "insert 1 cstack foo@bar.com"

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
		switch prepare_statement(input, &statement) {
		case enums.PrepareResult.PREPARE_SUCCESS:
			fmt.Printf("Success input SQL '%s'.\n", input)
		case enums.PrepareResult.PREPARE_SYNTAX_ERROR:
			fmt.Printf("Syntax error. Could not parse statement.\n")
			continue loop
		case enums.PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue loop
		}
		ret := execute_statement(statement, &table)

		switch ret {
		case (enums.ExecuteResult.EXECUTE_SUCCESS):
			fmt.Printf("Executed.\n")
		case (enums.ExecuteResult.EXECUTE_TABLE_FULL):
			fmt.Printf("Error: Table full.\n")
		}

		fmt.Printf(" sql: %s.", input)
	}

	free_table(table)
	fmt.Println("End of the sql terminal!")
}
