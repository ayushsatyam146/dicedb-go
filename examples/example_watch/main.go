package main

import (
	"fmt"
	"sync"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

func main() {
	var wg sync.WaitGroup

	client, err := dicedb.NewClient("localhost", 7379, &wg)
	if err != nil {
		fmt.Println(err)
	}

	resp := client.Fire(&wire.Command{Cmd: "PING"})
	fmt.Println(resp)

	wg.Add(1)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k1", "v1"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k1"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k2", "v2"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k2"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "SET", Args: []string{"k3", "v3"}})
	fmt.Println(resp)

	resp = client.Fire(&wire.Command{Cmd: "GET", Args: []string{"k3"}})
	fmt.Println(resp)

	go func(client *dicedb.Client) {
		dicedb.ListenForMessages(client, func(message string) {
			fmt.Println("Received message from second watcher:", message)
		})
		wg.Done()
	}(client)

	wg.Wait()
}
