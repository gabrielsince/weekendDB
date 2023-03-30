package main

import (
	"fmt"
	"testing"
)

func TestNew_table(t *testing.T) {

	t2 := new_table()
	if t2 == (Table{}) {
		fmt.Println("get the uninitialized Table{}")
	} else {
		t.Error("unexpected actions!")
	}

	free_table(t2)
}
