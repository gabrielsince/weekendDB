package main

import (
	enums "13/enums"
	utils "13/utils"

	// "bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"unsafe"
)

//  Necesitamos una prueba de esfuerzo aqui
//  como más de 4000 filas de entrada sin problema.

// int的大小是和操作系统位数相关的,
// 如果是32位操作系统,
// int类型的大小是4字节;
// 如果是64位操作系统,int类型的大小就是8个字节

func print_row(row *enums.Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}

func print_constants() {
	fmt.Println("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Println("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Println("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Println("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Println("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func print_leaf_node(node []byte) {
	num_cells := leaf_node_num_cells(node)
	fmt.Println("leaf (size %d)\n", num_cells)
	var i uint32 = 0
	for ; i < num_cells; i++ {
		key := leaf_node_key(node, int32(i))
		fmt.Println("  - %d : %d\n", i, key)
	}
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

	if strings.HasPrefix(input, ".constants") {
		fmt.Println("Constant: \n")
		print_constants()
		return enums.MetaCommandResult.META_COMMAND_SUCCESS
	}

	if strings.HasPrefix(input, ".btree") {
		fmt.Println("Tree: \n")
		// print_leaf_node(get_page(&table.pager, 0))
		print_tree(&table.pager, 0, 0)
		return enums.MetaCommandResult.META_COMMAND_SUCCESS
	}

	return enums.MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepare_statement(input string, statement *enums.Statement) int {

	if strings.HasPrefix(input, "insert") {
		fmt.Println(input)
		var username, email string
		var id uint32
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
	// if table.num_rows >= TABLE_MAX_ROWS {
	// 	return enums.ExecuteResult.EXECUTE_TABLE_FULL
	// }

	node := get_page(&table.pager, table.root_page_num)
	num_cells := leaf_node_num_cells(node)

	// if num_cells >= uint32(LEAF_NODE_MAX_CELLS) {
	// 	return enums.ExecuteResult.EXECUTE_TABLE_FULL
	// }

	row_to_insert := &(statement.Row_to_insert)
	// cursor := table_end(table)

	key_to_insert := row_to_insert.Id
	cursor := table_find(table, uint32(key_to_insert))

	if cursor.cell_num < int32(num_cells) {
		key_at_index := leaf_node_key(node, cursor.cell_num)
		if key_at_index == int32(key_to_insert) {
			return enums.ExecuteResult.EXECUTE_DUPLICATE_KEY
		}
	}

	leaf_node_insert(&cursor, uint32(row_to_insert.Id), row_to_insert)

	// // page_num, offset := row_slot_tuple(table, table.num_rows)
	// page_num, offset := cursor_value_tuple(&cursor)

	// // b := table.pager.pages[page_num][offset:]
	// b := get_page(&table.pager, int32(page_num))[offset:]
	// serialize_row(row_to_insert, &b)

	// // serialize_row(row_to_insert, row_slot(table, table.num_rows))
	// table.num_rows += 1
	// // free(cursor)

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

// EMAIL_SIZE: 256->255, para obtener 14 filas en una página.
const (
	ID_SIZE         = 4
	USERNAME_SIZE   = 32
	EMAIL_SIZE      = 255
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_OFFSET
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

// +/*
// + * Common Node Header Layout
// + */
// +const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
// +const uint32_t NODE_TYPE_OFFSET = 0;
// +const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
// +const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
// +const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
// +const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
// +const uint8_t COMMON_NODE_HEADER_SIZE =
// +    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const (
	NODE_TYPE_SIZE          = int32(unsafe.Sizeof(uint8(0)))
	NODE_TYPE_OFFSET        = 0
	IS_ROOT_SIZE            = int32(unsafe.Sizeof(uint8(0)))
	IS_ROOT_OFFSET          = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE     = int32(unsafe.Sizeof(uint32(0)))
	PARENT_POINTER_OFFSET   = IS_ROOT_SIZE + IS_ROOT_OFFSET
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// +/*
// + * Leaf Node Header Layout
// + */
// +const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
// +const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
// +const uint32_t LEAF_NODE_HEADER_SIZE =
// +    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const (
	LEAF_NODE_NUM_CELLS_SIZE   = int32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	// LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_NEXT_LEAF_SIZE   = int32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE +
		LEAF_NODE_NEXT_LEAF_SIZE
)

// +/*
// + * Leaf Node Body Layout
// + */
// +const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
// +const uint32_t LEAF_NODE_KEY_OFFSET = 0;
// +const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
// +const uint32_t LEAF_NODE_VALUE_OFFSET =
// +    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
// +const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
// +const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
// +const uint32_t LEAF_NODE_MAX_CELLS =
// +    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// Go中的指针及与指针对指针的操作主要有以下三种：
// 一普通的指针类型，例如 var intptr *T，定义一个T类型指针变量。
// 二内置类型uintptr，本质是一个无符号的整型，它的长度是跟平台相关的，它的长度可以用来保存一个指针地址。
// 三是unsafe包提供的Pointer，表示可以指向任意类型的指针。

const (
	LEAF_NODE_KEY_SIZE        = int32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_KEY_OFFSET      = 0
	LEAF_NODE_VALUE_SIZE      = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET    = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE       = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = int32(LEAF_NODE_SPACE_FOR_CELLS) / LEAF_NODE_CELL_SIZE
)

// Accessing Leaf Node Fields
// +uint32_t* leaf_node_num_cells(void* node) {
// +  return node + LEAF_NODE_NUM_CELLS_OFFSET;
// +}
// +
// +void* leaf_node_cell(void* node, uint32_t cell_num) {
// +  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
// +}
// +
// +uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
// +  return leaf_node_cell(node, cell_num);
// +}
// +
// +void* leaf_node_value(void* node, uint32_t cell_num) {
// +  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
// +}
// +
// +void initialize_leaf_node(void* node) { *leaf_node_num_cells(node) = 0; }
// +

func leaf_node_num_cells(node []byte) uint32 {
	ret, err := utils.BytesToInt(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+4], false)
	if err != nil {
		fmt.Println(err)
	}
	return uint32(ret)
}

func inc_leaf_node_num_cells(node []byte) uint32 {

	ret, err := utils.BytesToInt(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+4], false)
	if err != nil {
		fmt.Println(err)
	}
	ret = ret + 1
	b2, err := utils.IntToBytes(int(ret), 4)

	if err != nil {
		fmt.Println(err)
	}
	copy(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+4], b2)
	return uint32(ret)
}

func set_leaf_node_num_cells(node []byte, num int32) {

	// ret, err := utils.BytesToInt(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+4], false)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// ret = ret + 1
	b2, err := utils.IntToBytes(int(num), 4)

	if err != nil {
		fmt.Println(err)
	}
	copy(node[LEAF_NODE_NUM_CELLS_OFFSET:LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE], b2)
}

func get_leaf_node_next_leaf(node []byte) uint32 {

	ret, err := utils.BytesToInt(node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE], false)
	if err != nil {
		fmt.Println(err)
	}
	return uint32(ret)

}

func set_leaf_node_next_leaf(node []byte, val uint32) {

	b2, err := utils.IntToBytes(int(val), byte(LEAF_NODE_NEXT_LEAF_SIZE))

	if err != nil {
		fmt.Println(err)
	}
	copy(node[LEAF_NODE_NEXT_LEAF_OFFSET:LEAF_NODE_NEXT_LEAF_OFFSET+LEAF_NODE_NEXT_LEAF_SIZE], b2)

	// return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

func leaf_node_cell(node []byte, cell_num int32) []byte {
	return node[LEAF_NODE_HEADER_SIZE+cell_num*LEAF_NODE_CELL_SIZE:]
}

func leaf_node_key(node []byte, cell_num int32) int32 {
	b := leaf_node_cell(node, cell_num)
	ret, err := utils.BytesToInt(b[:4], true)
	if err != nil {
		fmt.Println(err)
	}
	return ret
}

/**
*   b2 作为返回值，没有直接影响到
**/
func set_leaf_node_key(node []byte, cell_num int32, new_key int32) {
	b := leaf_node_cell(node, cell_num)
	b2, err := utils.IntToBytes(int(new_key), 4)

	if err != nil {
		fmt.Println(err)
	}
	copy(b, b2)
}

func leaf_node_value(node []byte, cell_num int32) []byte {
	b := leaf_node_cell(node, cell_num)
	return b[LEAF_NODE_KEY_SIZE:]
}

func initialize_leaf_node(node []byte) {
	set_node_type(node, uint8(enums.NodeType.NODE_LEAF))
	set_node_root(node, false)
	set_leaf_node_num_cells(node, 0)

	set_leaf_node_next_leaf(node, 0) // 0 represents no sibling
}

// +void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
// 	+  void* node = get_page(cursor->table->pager, cursor->page_num);
// 	+
// 	+  uint32_t num_cells = *leaf_node_num_cells(node);
// 	+  if (num_cells >= LEAF_NODE_MAX_CELLS) {
// 	+    // Node full
// 	+    printf("Need to implement splitting a leaf node.\n");
// 	+    exit(EXIT_FAILURE);
// 	+  }
// 	+
// 	+  if (cursor->cell_num < num_cells) {
// 	+    // Make room for new cell
// 	+    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
// 	+      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
// 	+             LEAF_NODE_CELL_SIZE);
// 	+    }
// 	+  }
// 	+
// 	+  *(leaf_node_num_cells(node)) += 1;
// 	+  *(leaf_node_key(node, cursor->cell_num)) = key;
// 	+  serialize_row(value, leaf_node_value(node, cursor->cell_num));
// 	+}
// 	+

func leaf_node_insert(cursor *Cursor, key uint32, value *enums.Row) {
	node := get_page(&cursor.table.pager, int32(cursor.page_num))
	num_cells := int32(leaf_node_num_cells(node))

	if int32(num_cells) >= LEAF_NODE_MAX_CELLS {
		// Node full
		// fmt.Println("Need to implement splitting a leaf node.")
		// os.Exit(1)
		leaf_node_split_and_insert(cursor, key, value)
		return
	}

	if cursor.cell_num < num_cells {
		// Make room for new cell
		for i := num_cells; i > cursor.cell_num; i-- {
			copy(leaf_node_cell(node, i)[:LEAF_NODE_CELL_SIZE],
				leaf_node_cell(node, i-1)[:LEAF_NODE_CELL_SIZE])
		}
	}

	// new_num_cells := leaf_node_num_cells(node)
	// new_num_cells += 1
	inc_leaf_node_num_cells(node)

	set_leaf_node_key(node, cursor.cell_num, int32(key))
	// new_node_key := leaf_node_key(node, cursor.cell_num)
	// new_node_key = int32(key)

	b := leaf_node_value(node, cursor.cell_num)
	serialize_row(value, &b)
}

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
	// ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	// TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Pager struct {
	file_descriptor *os.File
	file_length     int64
	// pages           [TABLE_MAX_PAGES][PAGE_SIZE]byte
	num_pages int32
	pages     [TABLE_MAX_PAGES][PAGE_SIZE]byte
}

type Table struct {
	// num_rows int
	root_page_num int32
	pager         Pager
}

type Cursor struct {
	table *Table
	// row_num      int
	page_num     int32
	cell_num     int32
	end_of_table bool
}

// func table_start(table *Table) Cursor {
// 	var cursor = Cursor{}
// 	cursor.table = table
// 	// cursor.row_num = (table.num_rows == 0)
// 	// cursor.end_of_table = false

// 	cursor.page_num = int32(table.root_page_num)
// 	cursor.cell_num = 0
// 	root_node := get_page(&table.pager, table.root_page_num)

// 	num_cells := leaf_node_num_cells(root_node)
// 	cursor.end_of_table = (num_cells == 0)

// 	return cursor
// }

func table_start(table *Table) Cursor {
	var cursor = table_find(table, 0)

	var node = get_page(&table.pager, cursor.page_num)
	var num_cells = leaf_node_num_cells(node)
	cursor.end_of_table = (num_cells == 0)

	return cursor
}

func table_end(table *Table) Cursor {
	var cursor = Cursor{}
	cursor.table = table
	// cursor.row_num = table.num_rows
	cursor.page_num = int32(table.root_page_num)

	root_node := get_page(&table.pager, table.root_page_num)
	num_cells := leaf_node_num_cells(root_node)
	cursor.cell_num = int32(num_cells)

	cursor.end_of_table = true
	return cursor
}

// /*
// +Return the position of the given key.
// +If the key is not present, return the position
// +where it should be inserted
// +*/
// Cursor* table_find(Table* table, uint32_t key) {
// 	  uint32_t root_page_num = table->root_page_num;
// 	  void* root_node = get_page(table->pager, root_page_num);

// 	  if (get_node_type(root_node) == NODE_LEAF) {
// 	    return leaf_node_find(table, root_page_num, key);
// 	  } else {
// 	    printf("Need to implement searching an internal node\n");
// 	    exit(EXIT_FAILURE);
// 	 }
// }

func table_find(table *Table, key uint32) Cursor {
	var root_page_num = table.root_page_num
	var root_node = get_page(&table.pager, root_page_num)

	if get_node_type(root_node) == int32(enums.NodeType.NODE_LEAF) {
		return leaf_node_find(table, root_page_num, (int32)(key))
	} else {
		return internal_node_find(table, uint32(root_page_num), key)
		// fmt.Println("Need to implement searching an internal node")
		// os.Exit(1)
		// return Cursor{}
	}
}

// +Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
// 	+  void* node = get_page(table->pager, page_num);
// 	+  uint32_t num_keys = *internal_node_num_keys(node);
// 	+
// 	+  /* Binary search to find index of child to search */
// 	+  uint32_t min_index = 0;
// 	+  uint32_t max_index = num_keys; /* there is one more child than key */
// 	+
// 	+  while (min_index != max_index) {
// 	+    uint32_t index = (min_index + max_index) / 2;
// 	+    uint32_t key_to_right = *internal_node_key(node, index);
// 	+    if (key_to_right >= key) {
// 	+      max_index = index;
// 	+    } else {
// 	+      min_index = index + 1;
// 	+    }
// 	+  }
// +  uint32_t child_num = *internal_node_child(node, min_index);
// +  void* child = get_page(table->pager, child_num);
// +  switch (get_node_type(child)) {
// +    case NODE_LEAF:
// +      return leaf_node_find(table, child_num, key);
// +    case NODE_INTERNAL:
// +      return internal_node_find(table, child_num, key);
// +  }
// +}

func internal_node_find(table *Table, page_num uint32, key uint32) Cursor {

	var node = get_page(&table.pager, int32(page_num))

	var child_index = internal_node_find_child(node, key)
	var child_num = get_internal_node_child(node, child_index)

	// var node = get_page(&table.pager, int32(page_num))
	// var num_keys = get_internal_node_num_keys(node)

	// /* Binary search to find index of child to search */
	// var min_index = int32(0)
	// var max_index = num_keys /* there is one more child than key */

	// for min_index != max_index {
	// 	var index = (min_index + max_index) / 2
	// 	var key_to_right = get_internal_node_key(node, uint32(index))
	// 	if key_to_right >= int32(key) {
	// 		max_index = index
	// 	} else {
	// 		min_index = index + 1
	// 	}
	// }

	// var child_num = get_internal_node_child(node, uint32(min_index))
	var child = get_page(&table.pager, int32(child_num))
	switch get_node_type(child) {
	case enums.NodeType.NODE_LEAF:
		return leaf_node_find(table, int32(child_num), int32(key))
	case enums.NodeType.NODE_INTERNAL:
		return internal_node_find(table, child_num, key)
	}
	return Cursor{}
}

func internal_node_find_child(node []byte, key uint32) uint32 {
	// var node = get_page(&table.pager, int32(page_num))
	var num_keys = get_internal_node_num_keys(node)

	/* Binary search*/
	var min_index = int32(0)
	var max_index = num_keys /* there is one more child than key */

	for min_index != max_index {
		var index = (min_index + max_index) / 2
		var key_to_right = get_internal_node_key(node, uint32(index))
		if key_to_right >= int32(key) {
			max_index = index
		} else {
			min_index = index + 1
		}
	}

	return uint32(min_index)
	// var child_num = get_internal_node_child(node, uint32(min_index))
	// var child = get_page(&table.pager, int32(child_num))
	// switch get_node_type(child) {
	// case enums.NodeType.NODE_LEAF:
	// 	return leaf_node_find(table, int32(child_num), int32(key))
	// case enums.NodeType.NODE_INTERNAL:
	// 	return internal_node_find(table, child_num, key)
	// }
	// return Cursor{}
}

func internal_node_insert(table *Table, parent_page_num int32,
	child_page_num int32) {
	/*
	  Add a new child/key pair to parent that corresponds to child
	*/

	var parent = get_page(&table.pager, parent_page_num)
	var child = get_page(&table.pager, child_page_num)
	var child_max_key = get_node_max_key(child)
	var index = internal_node_find_child(parent, uint32(child_max_key))

	var original_num_keys = get_internal_node_num_keys(parent)
	set_internal_node_num_keys(parent, int(original_num_keys+1))

	if original_num_keys >= INTERNAL_NODE_MAX_CELLS {
		fmt.Printf("Need to implement splitting internal node\n")
		os.Exit(1)
	}

	var right_child_page_num = get_internal_node_right_child(parent)
	var right_child = get_page(&table.pager, int32(right_child_page_num))

	if child_max_key > get_node_max_key(right_child) {
		/* Replace right child */
		set_internal_node_child(parent, uint32(original_num_keys), right_child_page_num)
		set_internal_node_key(parent, uint32(original_num_keys), uint32(get_node_max_key(right_child)))
		set_internal_node_right_child(parent, uint32(child_page_num))

	} else {
		/* Make room for the new cell */
		for i := uint32(original_num_keys); i > index; i-- {
			var destination = internal_node_cell(parent, i)
			var source = internal_node_cell(parent, i-1)
			copy(destination[:INTERNAL_NODE_CELL_SIZE], source)
		}
		set_internal_node_child(parent, index, uint32(child_page_num))
		set_internal_node_key(parent, index, uint32(child_max_key))
	}
}

// +Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
// 	+  void* node = get_page(table->pager, page_num);
// 	+  uint32_t num_cells = *leaf_node_num_cells(node);
// 	+
// 	+  Cursor* cursor = malloc(sizeof(Cursor));
// 	+  cursor->table = table;
// 	+  cursor->page_num = page_num;
// 	+
// 	+  // Binary search
// 	+  uint32_t min_index = 0;
// 	+  uint32_t one_past_max_index = num_cells;
// 	+  while (one_past_max_index != min_index) {
// 	+    uint32_t index = (min_index + one_past_max_index) / 2;
// 	+    uint32_t key_at_index = *leaf_node_key(node, index);
// 	+    if (key == key_at_index) {
// 	+      cursor->cell_num = index;
// 	+      return cursor;
// 	+    }
// 	+    if (key < key_at_index) {
// 	+      one_past_max_index = index;
// 	+    } else {
// 	+      min_index = index + 1;
// 	+    }
// 	+  }
// 	+
// 	+  cursor->cell_num = min_index;
// 	+  return cursor;
// 	+}

func leaf_node_find(table *Table, page_num int32, key int32) Cursor {

	var node = get_page(&table.pager, page_num)
	var num_cells = leaf_node_num_cells(node)

	var cursor = Cursor{}
	cursor.table = table
	cursor.page_num = page_num

	// Binary search
	var min_index uint32 = 0
	var one_past_max_index = num_cells
	for one_past_max_index != min_index {
		var index int32 = (int32)((min_index + one_past_max_index) / 2)
		var key_at_index = leaf_node_key(node, index)
		if key == key_at_index {
			cursor.cell_num = index
			return cursor
		}
		if key < key_at_index {
			one_past_max_index = (uint32)(index)
		} else {
			min_index = (uint32)(index + 1)
		}
	}

	cursor.cell_num = (int32)(min_index)
	return cursor
}

// NodeType get_node_type(void* node) {
// 	uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
// 	return (NodeType)value;
// }

// void set_node_type(void* node, NodeType type) {
// 	uint8_t value = type;
// 	*((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
// }

func get_node_type(node []byte) int32 {

	// uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
	// return (NodeType)value;

	ret, err := utils.BytesToInt(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET+1], false)
	if err != nil {
		fmt.Println(err)
	}
	return ret
}

// b := leaf_node_cell(node, cell_num)
// b2, err := utils.IntToBytes(int(new_key), 4)

// if err != nil {
// 	fmt.Println(err)
// }
// copy(b, b2)

func set_node_type(node []byte, nodeType uint8) {

	b2, err := utils.IntToBytes(int(nodeType), 1)
	if err != nil {
		fmt.Println(err)
	}
	copy(node[NODE_TYPE_OFFSET:NODE_TYPE_OFFSET+1], b2)

	// uint8_t value = type;
	// *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

func cursor_value(cursor *Cursor) []byte {
	// row_num := cursor.row_num
	// page_num := row_num / ROWS_PER_PAGE
	page_num := cursor.page_num
	page := get_page(&cursor.table.pager, int32(page_num))

	// row_offset := row_num % ROWS_PER_PAGE
	// byte_offset := row_offset * ROW_SIZE
	// return page[byte_offset:]
	return leaf_node_value(page, int32(cursor.cell_num))
}

func cursor_advance(cursor *Cursor) {
	// cursor.row_num += 1
	// if cursor.row_num >= cursor.table.num_rows {
	// cursor.end_of_table = true
	// }

	page_num := cursor.page_num
	node := get_page(&cursor.table.pager, int32(page_num))
	cursor.cell_num += 1

	if uint32(cursor.cell_num) >= leaf_node_num_cells(node) {
		// cursor.end_of_table = true

		/* Advance to next leaf node */
		var next_page_num = get_leaf_node_next_leaf(node)
		if next_page_num == 0 {
			/* This was rightmost leaf */
			cursor.end_of_table = true
		} else {
			cursor.page_num = int32(next_page_num)
			cursor.cell_num = 0
		}
	}
}

// func row_slot(table *Table, row_num int) []byte {
// 	page_num := row_num / ROWS_PER_PAGE
// 	page := table.pager.pages[page_num]
// 	row_offset := row_num % ROWS_PER_PAGE
// 	byte_offset := row_offset * ROW_SIZE
// 	return page[byte_offset:]
// }

// func cursor_value_tuple(cursor *Cursor) (int, int) {
// 	row_num := cursor.row_num
// 	page_num := row_num / ROWS_PER_PAGE
// 	// page := get_page(&cursor.table.pager, int32(page_num))
// 	row_offset := row_num % ROWS_PER_PAGE
// 	byte_offset := row_offset * ROW_SIZE
// 	return page_num, byte_offset
// }

// func row_slot_tuple(table *Table, row_num int) (int, int) {
// 	page_num := row_num / ROWS_PER_PAGE
// 	// page := table.pages[page_num]
// 	row_offset := row_num % ROWS_PER_PAGE
// 	byte_offset := row_offset * ROW_SIZE
// 	return page_num, byte_offset
// }

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
	pager.num_pages = int32(fileInfo.Size() / PAGE_SIZE)

	if fileInfo.Size()%PAGE_SIZE != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.")
		os.Exit(1)
	}

	for page_i := 0; page_i < int(pager.num_pages); page_i++ {
		_, err := pager.file_descriptor.ReadAt(pager.pages[page_i][:], int64(PAGE_SIZE*page_i))
		if err != nil {
			fmt.Println(page_i, " read page failed! ", err)
		}
	}

	// pager.pages nothing
	return pager
}

func db_open(filename string) Table {
	fmt.Println("malloc the table!")
	pager := pager_open(filename)
	// num_rows := pager.file_length / ROW_SIZE
	table := Table{}
	// table.num_rows = int(num_rows)
	table.pager = pager
	table.root_page_num = 0
	if table.pager.num_pages == 0 {
		// new database file. Initialize page 0 as leaf node
		root_node := get_page(&table.pager, 0)
		initialize_leaf_node(root_node)
	}
	return table
}

// func get_page(pager *Pager, page_num int32) []byte {

// 	// no cache missed. load data from file everytime
// 	if page_num > TABLE_MAX_PAGES {
// 		fmt.Println("Tried to fetch page number out of bounds.%d > %d\n", page_num, TABLE_MAX_PAGES)
// 		os.Exit(1)
// 	}

// 	var num_pages int64 = pager.file_length / PAGE_SIZE
// 	var realLen = pager.file_length - PAGE_SIZE*num_pages

// 	if pager.file_length > PAGE_SIZE*num_pages {
// 		num_pages += 1
// 	}

// 	if page_num < int32(num_pages) {
// 		_, err := pager.file_descriptor.ReadAt(pager.pages[page_num][:realLen], int64(page_num*PAGE_SIZE))
// 		if err != nil {
// 			fmt.Println(" read page failed! ", err)
// 		}
// 	}

// 	if page_num >= pager.num_pages {
// 		pager.num_pages = page_num + 1
// 	}

// 	return pager.pages[:]
// 	// return pager.pages[page_num][:]
// }

// func get_page(pager *Pager, page_num int32) []byte {

// 	// no cache missed. load data from file everytime
// 	if page_num > TABLE_MAX_PAGES {
// 		fmt.Println("Tried to fetch page number out of bounds.%d > %d\n", page_num, TABLE_MAX_PAGES)
// 		os.Exit(1)
// 	}

// 	var num_pages int64 = pager.file_length / PAGE_SIZE
// 	var realLen = pager.file_length - PAGE_SIZE*num_pages

// 	if pager.file_length > PAGE_SIZE*num_pages {
// 		num_pages += 1
// 	}

// 	if page_num < int32(num_pages) {
// 		_, err := pager.file_descriptor.ReadAt(pager.pages[page_num][:realLen], int64(page_num*PAGE_SIZE))
// 		if err != nil {
// 			fmt.Println(" read page failed! ", err)
// 		}
// 	}

// 	if page_num >= pager.num_pages {
// 		pager.num_pages = page_num + 1
// 	}

// 	return pager.pages[:]
// 	// return pager.pages[page_num][:]
// }

func get_page(pager *Pager, page_num int32) []byte {

	// no cache missed. load data from file everytime
	if page_num > TABLE_MAX_PAGES {
		fmt.Println("Tried to fetch page number out of bounds.%d > %d", page_num, TABLE_MAX_PAGES)
		os.Exit(1)
	}

	if page_num >= pager.num_pages {
		pager.num_pages = page_num + 1
	}

	return pager.pages[page_num][:]
}

func db_close(table *Table) {
	pager := table.pager
	// var num_full_pages int = table.num_rows / ROWS_PER_PAGE
	// for i := 0; i < num_full_pages; i++ {
	for i := 0; i < int(pager.num_pages); i++ {
		pager_flush(&pager, i)
	}

	// // The partial page to write to the end of the file is not needed again if we swtich to a B-tree
	// num_additional_rows := table.num_rows % ROWS_PER_PAGE
	// if num_additional_rows > 0 {
	// 	var page_num int = num_full_pages
	// 	pager_flush(&pager, page_num, num_additional_rows*ROW_SIZE)

	// }

	err := pager.file_descriptor.Close()
	if err != nil {
		fmt.Println("close db failed! ", err)
	}

}

// func pager_flush(pager *Pager, page_num int, size int) {

// 	_, err := pager.file_descriptor.WriteAt(pager.pages[page_num][:size], int64(page_num*PAGE_SIZE))
// 	if err != nil {
// 		fmt.Println("write page failed! ", err)
// 	}
// }

func pager_flush(pager *Pager, page_num int) {

	_, err := pager.file_descriptor.WriteAt(pager.pages[page_num][:PAGE_SIZE], int64(page_num*PAGE_SIZE))
	if err != nil {
		fmt.Println("write page failed! ", err)
	}
}

// +const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
// +const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
// +    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

const (
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT
)

func get_node_parent(node []byte) int32 {
	ret, err := utils.BytesToInt(node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE], false)
	if err != nil {
		fmt.Println(err)
	}
	return ret
}

func set_node_parent(node []byte, val int32) {

	b2, err := utils.IntToBytes(int(val), byte(PARENT_POINTER_SIZE))
	if err != nil {
		fmt.Println(err)
	}
	copy(node[PARENT_POINTER_OFFSET:PARENT_POINTER_OFFSET+PARENT_POINTER_SIZE], b2)

	// copy(node[PARENT_POINTER_OFFSET:PARENT_POINTER_SIZE], setnum)
	// return node[PARENT_POINTER_OFFSET:]
}

func leaf_node_split_and_insert(cursor *Cursor, key uint32, value *enums.Row) {
	/*
		Create a new node and move half the cells over.
		Insert the new value in one of the two nodes.
		Update parent or create a new parent.
	*/

	var old_node = get_page(&cursor.table.pager, cursor.page_num)
	var old_max = get_node_max_key(old_node)
	var new_page_num = get_unused_page_num(&cursor.table.pager)
	var new_node = get_page(&cursor.table.pager, new_page_num)
	initialize_leaf_node(new_node)
	b := get_node_parent(old_node)
	set_node_parent(new_node, b)

	u := get_leaf_node_next_leaf(old_node)
	set_leaf_node_next_leaf(new_node, u)
	set_leaf_node_next_leaf(old_node, uint32(new_page_num))

	/*
		All existing keys plus new key should be divided
		evenly between old (left) and new (right) nodes.
		Starting from the right, move each key to correct position.
	*/
	for i := LEAF_NODE_MAX_CELLS; i >= 0; i-- {
		var destination_node []byte
		if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
			destination_node = new_node
		} else {
			destination_node = old_node
		}
		var index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT
		var destination []byte = leaf_node_cell(destination_node, index_within_node)

		if i == cursor.cell_num {
			b2, err := utils.IntToBytes(int(key), byte(LEAF_NODE_KEY_SIZE))
			if err != nil {
				fmt.Println(err)
			}
			copy(destination[:LEAF_NODE_KEY_SIZE], b2)
			b := destination[LEAF_NODE_KEY_SIZE:]
			serialize_row(value, &b)
		} else if i > cursor.cell_num {
			//memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE)
			b := leaf_node_cell(old_node, i-1)
			copy(destination[:LEAF_NODE_CELL_SIZE], b)
		} else {
			// memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE)
			b := leaf_node_cell(old_node, i)
			copy(destination[:LEAF_NODE_CELL_SIZE], b)
		}
		fmt.Println(index_within_node)
	}

	/* Update cell count on both leaf nodes */
	set_leaf_node_num_cells(old_node, (LEAF_NODE_LEFT_SPLIT_COUNT))
	set_leaf_node_num_cells(new_node, (LEAF_NODE_RIGHT_SPLIT_COUNT))

	if is_node_root(old_node) {
		create_new_root(cursor.table, uint32(new_page_num))
	} else {
		// fmt.Printf("Need to implement updating parent after split\n")
		// os.Exit(1)
		var parent_page_num = get_node_parent(old_node)
		var new_max = get_node_max_key(old_node)
		var parent = get_page(&cursor.table.pager, parent_page_num)

		update_internal_node_key(parent, uint32(old_max), uint32(new_max))
		internal_node_insert(cursor.table, parent_page_num, new_page_num)
	}
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
func get_unused_page_num(pager *Pager) int32 { return pager.num_pages }

func create_new_root(table *Table, right_child_page_num uint32) {
	/*
		Handle splitting the root.
		Old root copied to new page, becomes left child.
		Address of right child passed in.
		Re-initialize root page to contain the new root node.
		New root node points to two children.
	*/

	var root = get_page(&table.pager, table.root_page_num)
	var right_child = get_page(&table.pager, int32(right_child_page_num))
	var left_child_page_num = get_unused_page_num(&table.pager)
	var left_child = get_page(&table.pager, left_child_page_num)

	/* Left child has data copied from old root */
	// memcpy(left_child, root, PAGE_SIZE);
	copy(left_child, root[:PAGE_SIZE])
	set_node_root(left_child, false)

	/* Root node is a new internal node with one key and two children */
	initialize_internal_node(root)
	set_node_root(root, true)
	set_internal_node_num_keys(root, 1)
	set_internal_node_child(root, 0, uint32(left_child_page_num))
	var left_child_max_key = get_node_max_key(left_child)
	set_internal_node_key(root, 0, uint32(left_child_max_key))
	set_internal_node_right_child(root, right_child_page_num)

	set_node_parent(left_child, table.root_page_num)
	set_node_parent(right_child, table.root_page_num)
}

// +/*
// + * Internal Node Header Layout
// + */
// +const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
// +const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
// +const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
// +const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
// +    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
// +const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
// +                                           INTERNAL_NODE_NUM_KEYS_SIZE +
// +                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Header Layout
 */

const (
	INTERNAL_NODE_NUM_KEYS_SIZE      = int32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = int32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE +
		INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE
)

// +/*
// + * Internal Node Body Layout
// + */
// +const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
// +const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
// +const uint32_t INTERNAL_NODE_CELL_SIZE =
// +    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

/*
 * Internal Node Body Layout
 */
const (
	INTERNAL_NODE_KEY_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_CHILD_SIZE = uint32(unsafe.Sizeof(uint32(0)))
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
	INTERNAL_NODE_MAX_CELLS  = 3
)

func internal_node_num_keys(node []byte) []byte {
	return node[:INTERNAL_NODE_NUM_KEYS_OFFSET]
}

func set_internal_node_num_keys(node []byte, num int) {

	b2, err := utils.IntToBytes(int(num), byte(INTERNAL_NODE_NUM_KEYS_SIZE))
	if err != nil {
		fmt.Println(err)
	}
	copy(node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], b2)

	// return node[INTERNAL_NODE_NUM_KEYS_OFFSET]
}

func get_internal_node_num_keys(node []byte) int32 {

	ret, err := utils.BytesToInt(node[INTERNAL_NODE_NUM_KEYS_OFFSET:INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE], false)
	if err != nil {
		fmt.Println(err)
	}
	return ret
	// return node[INTERNAL_NODE_NUM_KEYS_OFFSET]
}

func internal_node_right_child(node []byte) []byte {
	return node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:]
}

func set_internal_node_right_child(node []byte, num uint32) {

	b2, err := utils.IntToBytes(int(num), byte(INTERNAL_NODE_RIGHT_CHILD_SIZE))
	if err != nil {
		fmt.Println(err)
	}
	copy(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE], b2)

	// return node[INTERNAL_NODE_RIGHT_CHILD_OFFSET]
}

func get_internal_node_right_child(node []byte) uint32 {
	b := node[INTERNAL_NODE_RIGHT_CHILD_OFFSET : INTERNAL_NODE_RIGHT_CHILD_OFFSET+INTERNAL_NODE_RIGHT_CHILD_SIZE]
	ret, err := utils.BytesToInt(b, false)
	if err != nil {
		fmt.Println(err)
	}
	return uint32(ret)
}

func internal_node_cell(node []byte, cell_num uint32) []byte {
	return node[uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE : uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE+INTERNAL_NODE_CELL_SIZE]
}

func set_internal_node_cell(node []byte, cell_num uint32, set_num uint32) {

	b2, err := utils.IntToBytes(int(set_num), byte(INTERNAL_NODE_CHILD_SIZE))
	if err != nil {
		fmt.Println(err)
	}
	copy(node[uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE:uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE+INTERNAL_NODE_CHILD_SIZE], b2)

	// return node[INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE]
}

func get_internal_node_cell(node []byte, cell_num uint32) []byte {
	return node[uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE : uint32(INTERNAL_NODE_HEADER_SIZE)+cell_num*INTERNAL_NODE_CELL_SIZE+INTERNAL_NODE_CELL_SIZE]
}

func get_internal_node_child(node []byte, child_num uint32) uint32 {
	var num_keys = get_internal_node_num_keys(node)
	var b []byte
	if child_num > uint32(num_keys) {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys)
		os.Exit(1)
	} else if child_num == uint32(num_keys) {
		b = internal_node_right_child(node)
	} else {
		b = get_internal_node_cell(node, child_num)
	}
	ret, err := utils.BytesToInt(b[:INTERNAL_NODE_CHILD_SIZE], false)
	if err != nil {
		fmt.Println(err)
	}
	return uint32(ret)
}

func internal_node_child(node []byte, child_num uint32) []byte {
	var num_keys = get_internal_node_num_keys(node)
	if child_num > uint32(num_keys) {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys)
		os.Exit(1)
		return []byte{0}
	} else if child_num == uint32(num_keys) {
		return internal_node_right_child(node)
	} else {
		return get_internal_node_cell(node, child_num)
	}
}

func set_internal_node_child(node []byte, child_num uint32, set_num uint32) {
	var num_keys = get_internal_node_num_keys(node)
	if child_num > uint32(num_keys) {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys)
		os.Exit(1)
	} else if child_num == uint32(num_keys) {
		set_internal_node_right_child(node, set_num)
	} else {
		set_internal_node_cell(node, child_num, set_num)
	}
}

func internal_node_key(node []byte, key_num uint32) []byte {
	b := internal_node_cell(node, key_num)
	return b[INTERNAL_NODE_CHILD_SIZE:]
	// return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE
}

func set_internal_node_key(node []byte, key_num uint32, set_num uint32) {

	retByte := get_internal_node_cell(node, key_num)

	b2, err := utils.IntToBytes(int(set_num), byte(INTERNAL_NODE_CHILD_SIZE))
	if err != nil {
		fmt.Println(err)
	}
	copy(retByte[INTERNAL_NODE_CHILD_SIZE:], b2)

	// return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE
}

func get_internal_node_key(node []byte, key_num uint32) int32 {

	retByte := get_internal_node_cell(node, key_num)

	ret, err := utils.BytesToInt(retByte[INTERNAL_NODE_CHILD_SIZE:], false)
	if err != nil {
		fmt.Println(err)
	}
	return ret

	// return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE
}

func get_node_max_key(node []byte) int32 {
	switch get_node_type(node) {
	case enums.NodeType.NODE_INTERNAL:
		return get_internal_node_key(node, uint32(get_internal_node_num_keys(node))-1)
	case enums.NodeType.NODE_LEAF:
		return leaf_node_key(node, int32(leaf_node_num_cells(node))-1)
	}
	return 0
}

// Keeping Track of the Root

func is_node_root(node []byte) bool {

	ret, err := utils.BytesToInt(node[:IS_ROOT_OFFSET], false)

	if err != nil {
		fmt.Println(err)
	}
	return utils.GetBoolFromInt32(ret)

	//   uint8_t value := *((uint8_t*)(node + IS_ROOT_OFFSET));
	//   return (bool)value;
}

func set_node_root(node []byte, is_root bool) {

	b2, err := utils.IntToBytes(int(utils.GetInt32(is_root)), 1)
	if err != nil {
		fmt.Println(err)
	}
	copy(node[IS_ROOT_OFFSET:IS_ROOT_OFFSET+IS_ROOT_SIZE], b2)

	//   uint8_t value = is_root;
	//   *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

// void initialize_leaf_node(void* node) {
// 	set_node_type(node, NODE_LEAF);
//  +  set_node_root(node, false);
// 	*leaf_node_num_cells(node) = 0;
//   }

//  +void initialize_internal_node(void* node) {
//  +  set_node_type(node, NODE_INTERNAL);
//  +  set_node_root(node, false);
//  +  *internal_node_num_keys(node) = 0;
//  +}

func initialize_internal_node(node []byte) {
	set_node_type(node, uint8(enums.NodeType.NODE_INTERNAL))
	set_node_root(node, false)
	set_internal_node_num_keys(node, 0)
}

func update_internal_node_key(node []byte, old_key uint32, new_key uint32) {
	var old_child_index = internal_node_find_child(node, old_key)
	set_internal_node_key(node, old_child_index, new_key)
}

//      // New database file. Initialize page 0 as leaf node.
//      void* root_node = get_page(pager, 0);
//      initialize_leaf_node(root_node);
// +    set_node_root(root_node, true);
//    }

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf("  ")
	}
}

func print_tree(pager *Pager, page_num uint32, indentation_level uint32) {
	node := get_page(pager, int32(page_num))
	var (
		num_keys, child uint32
	)

	switch get_node_type(node) {
	case (enums.NodeType.NODE_LEAF):
		num_keys = leaf_node_num_cells(node)
		indent(indentation_level)
		fmt.Println("- leaf (size %d)", num_keys)
		for i := uint32(0); i < num_keys; i++ {
			indent(indentation_level + 1)
			fmt.Println("- %d", leaf_node_key(node, int32(i)))
		}
		break
	case (enums.NodeType.NODE_INTERNAL):
		num_keys := get_internal_node_num_keys(node)
		indent(indentation_level)
		fmt.Println("- internal (size %d)", num_keys)
		for i := int32(0); i < num_keys; i++ {
			child = get_internal_node_child(node, uint32(i))
			print_tree(pager, child, indentation_level+1)
			indent(indentation_level + 1)
			fmt.Println("- key %d", get_internal_node_key(node, uint32(i)))
		}
		child = get_internal_node_right_child(node)
		print_tree(pager, child, indentation_level+1)
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

	// inputReader := bufio.NewReader(os.Stdin)
loop:
	for {
		fmt.Print("db: >")
		// input, err := inputReader.ReadString('\n')
		// if err != nil {
		// 	fmt.Println("There were errors reading, exiting program.")
		// 	return
		// }
		// input_template := "insert #{i} user#{i} person#{i}@example.com"
		// input := strings.Replace(input_template, "#{i}", "30", -1)

		// input := "insert 1 cstack foo@bar.com"
		// input := "insert 2 cstack2 foo@bar.com"
		// input := "select"
		input := ".btree"

		if strings.HasPrefix(input, ".") {
			switch do_meta_command(input, &table) {
			case enums.MetaCommandResult.META_COMMAND_SUCCESS:
				break
				// continue loop
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
			break
			// continue loop
		}
		ret := execute_statement(statement, &table)

		switch ret {
		case (enums.ExecuteResult.EXECUTE_SUCCESS):
			fmt.Printf("Executed.\n")
		case (enums.ExecuteResult.EXECUTE_DUPLICATE_KEY):
			fmt.Printf("Error: Duplicate key.\n")
		case (enums.ExecuteResult.EXECUTE_TABLE_FULL):
			fmt.Printf("Error: Table full.\n")
		}

		fmt.Printf(" sql: %s.", input)
		// db_close(&table)
		break
	}

	// free_table(table)
	db_close(&table)
	fmt.Println("End of the sql terminal!")
}
