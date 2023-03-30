package enums

import (
	"fmt"
	"reflect"
)

func init() {
	fmt.Println("enums package init successful!")
}

type executeResult struct {
	EXECUTE_SUCCESS, EXECUTE_TABLE_FULL int
}

func (c executeResult) Get(id string) int {
	vo := reflect.ValueOf(c)
	typeVo := vo.Type()

	for i := 0; i < vo.NumField(); i++ {
		if typeVo.Field(i).Name == id {
			return vo.Field(i).Interface().(int)
		}
	}
	return 0
}

var ExecuteResult = executeResult{
	EXECUTE_SUCCESS:    0,
	EXECUTE_TABLE_FULL: 1,
}

type metaCommandResult struct {
	META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND int
}

func (c metaCommandResult) Get(id string) int {
	vo := reflect.ValueOf(c)
	typeVo := vo.Type()

	for i := 0; i < vo.NumField(); i++ {
		if typeVo.Field(i).Name == id {
			return vo.Field(i).Interface().(int)
		}
	}
	return 0
}

var MetaCommandResult = metaCommandResult{
	META_COMMAND_SUCCESS:              0,
	META_COMMAND_UNRECOGNIZED_COMMAND: 1,
}

type prepareResult struct {
	PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR int
}

func (c prepareResult) Get(id string) int {
	vo := reflect.ValueOf(c)
	typeVo := vo.Type()

	for i := 0; i < vo.NumField(); i++ {
		if typeVo.Field(i).Name == id {
			return vo.Field(i).Interface().(int)
		}
	}
	return 0
}

var PrepareResult = prepareResult{
	PREPARE_SUCCESS:                0,
	PREPARE_UNRECOGNIZED_STATEMENT: 1,
	PREPARE_SYNTAX_ERROR:           2,
}

type statementType struct {
	STATEMENT_INSERT, STATEMENT_SELECT int
}

func (c statementType) Get(id string) int {
	vo := reflect.ValueOf(c)
	typeVo := vo.Type()

	for i := 0; i < vo.NumField(); i++ {
		if typeVo.Field(i).Name == id {
			return vo.Field(i).Interface().(int)
		}
	}
	return 0
}

var StatementType = statementType{
	STATEMENT_INSERT: 0,
	STATEMENT_SELECT: 1,
}

type Row struct {
	Id       int8
	Username [32]byte
	Email    [256]byte
}

type Statement struct {
	Typ           int
	Row_to_insert Row
}
