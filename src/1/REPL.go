package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Begin of the sql terminal!")
	inputReader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("db: >")
		input, err := inputReader.ReadString('\n')
		if err != nil {
			fmt.Println("There were errors reading, exiting program.")
			return
		}
		res := strings.HasPrefix(input, ".exit")
		if res {
			break
		}
		fmt.Printf(" sql: %s.", input)
	}
	fmt.Println("End of the sql terminal!")
}
