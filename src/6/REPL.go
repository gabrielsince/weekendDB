package main

import (
	enums "6/enums"
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
func do_meta_command(input string, table *Table) int {
	if strings.HasPrefix(input, ".exit") {
		db_close(table)
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
	cursor := table_end(table)

	// page_num, offset := row_slot_tuple(table, table.num_rows)
	page_num, offset := cursor_value_tuple(&cursor)

	// b := table.pager.pages[page_num][offset:]
	b := get_page(&table.pager, int32(page_num))[offset:]
	serialize_row(row_to_insert, &b)

	// serialize_row(row_to_insert, row_slot(table, table.num_rows))
	table.num_rows += 1
	// free(cursor)

	return enums.ExecuteResult.EXECUTE_SUCCESS
}

func execute_select(statement enums.Statement, table *Table) int {
	cursor := table_start(table)
	var row enums.Row
	for !cursor.end_of_table {
		deserialize_row(cursor_value(&cursor), &row)
		print_row(&row)
		cursor_advance(&cursor)
	}
	// free(cursor)
	return enums.ExecuteResult.EXECUTE_SUCCESS
}

// func execute_select(statement enums.Statement, table *Table) int {
// 	var row enums.Row
// 	for i := 0; i < table.num_rows; i++ {
// 		deserialize_row(row_slot(table, i), &row)
// 		print_row(&row)
// 	}
// 	return enums.ExecuteResult.EXECUTE_SUCCESS
// }

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
}

func deserialize_row(source []byte, destination *enums.Row) {

	buf := bytes.NewBuffer(source)
	err := binary.Read(buf, binary.LittleEndian, destination)
	if err != nil {
		panic(err)
	}
}

const (
	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Pager struct {
	file_descriptor *os.File
	file_length     int64
	pages           [TABLE_MAX_PAGES][PAGE_SIZE]byte
}

type Table struct {
	num_rows int
	pager    Pager
}

type Cursor struct {
	table        *Table
	row_num      int
	end_of_table bool
}

func table_start(table *Table) Cursor {
	var cursor = Cursor{}
	cursor.table = table
	cursor.row_num = 0
	cursor.end_of_table = false
	return cursor
}

func table_end(table *Table) Cursor {
	var cursor = Cursor{}
	cursor.table = table
	cursor.row_num = table.num_rows
	cursor.end_of_table = true
	return cursor
}

func cursor_value(cursor *Cursor) []byte {
	row_num := cursor.row_num
	page_num := row_num / ROWS_PER_PAGE
	page := get_page(&cursor.table.pager, int32(page_num))
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	return page[byte_offset:]
}

func cursor_advance(cursor *Cursor) {
	cursor.row_num += 1
	if cursor.row_num >= cursor.table.num_rows {
		cursor.end_of_table = true
	}
}

func row_slot(table *Table, row_num int) []byte {
	page_num := row_num / ROWS_PER_PAGE
	page := table.pager.pages[page_num]
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	return page[byte_offset:]
}

func cursor_value_tuple(cursor *Cursor) (int, int) {
	row_num := cursor.row_num
	page_num := row_num / ROWS_PER_PAGE
	// page := get_page(&cursor.table.pager, int32(page_num))
	row_offset := row_num % ROWS_PER_PAGE
	byte_offset := row_offset * ROW_SIZE
	return page_num, byte_offset
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

func pager_open(filename string) Pager {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("OpenFile failed: ", err)
	}

	// defer file.Close()

	pager := Pager{}
	pager.file_descriptor = file

	fileInfo, err2 := os.Stat(filename)
	if err2 != nil {
		fmt.Println("Stat File failed: ", err2)
	}
	pager.file_length = fileInfo.Size()
	// pager.pages nothing
	return pager
}

func db_open(filename string) Table {
	fmt.Println("malloc the table!")
	pager := pager_open(filename)
	num_rows := pager.file_length / ROW_SIZE
	table := Table{}
	table.num_rows = int(num_rows)
	table.pager = pager
	return table
}

func get_page(pager *Pager, page_num int32) []byte {

	// no cache missed. load data from file everytime

	if page_num > TABLE_MAX_PAGES {
		fmt.Println("Tried to fetch page number out of bounds.%d > %d\n", page_num, TABLE_MAX_PAGES)
		os.Exit(1)
	}

	var num_pages int64 = pager.file_length / PAGE_SIZE
	var realLen = pager.file_length - PAGE_SIZE*num_pages

	if pager.file_length > PAGE_SIZE*num_pages {
		num_pages += 1
	}

	if page_num < int32(num_pages) {
		_, err := pager.file_descriptor.ReadAt(pager.pages[page_num][:realLen], int64(page_num*PAGE_SIZE))
		if err != nil {
			fmt.Println(" read page failed! ", err)
		}
	}
	return pager.pages[page_num][:]
}

func db_close(table *Table) {
	pager := table.pager
	var num_full_pages int = table.num_rows / ROWS_PER_PAGE
	for i := 0; i < num_full_pages; i++ {
		pager_flush(&pager, i, PAGE_SIZE)
	}

	// The partial page to write to the end of the file is not needed again if we swtich to a B-tree
	num_additional_rows := table.num_rows % ROWS_PER_PAGE
	if num_additional_rows > 0 {
		var page_num int = num_full_pages
		pager_flush(&pager, page_num, num_additional_rows*ROW_SIZE)

	}

	pager.file_descriptor.Close()
}

func pager_flush(pager *Pager, page_num int, size int) {

	_, err := pager.file_descriptor.WriteAt(pager.pages[page_num][:size], int64(page_num*PAGE_SIZE))
	if err != nil {
		fmt.Println("write page failed! ", err)
	}

}

func main() {
	//fmt.Println("Begin of the sql terminal!")
	//table := new_table()

	// fmt.Println("Begin of the sql terminal with persistent data !")
	// args := os.Args
	// arg_num := len(os.Args)

	// if arg_num < 2 {
	// 	fmt.Println("must contain the persistent data name!")
	// 	os.Exit(1)
	// }
	// table := db_open(args[1])

	table := db_open("./test.db")

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
			switch do_meta_command(input, &table) {
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
		// db_close(&table)
	}

	// free_table(table)
	db_close(&table)
	fmt.Println("End of the sql terminal!")
}
