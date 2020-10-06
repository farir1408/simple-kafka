package main

import (
	"log"

	"github.com/farir1408/simple-kafka/cmd/app/receiver"
	"github.com/farir1408/simple-kafka/cmd/app/sender"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{Use: "app", TraverseChildren: true}

func main() {
	rootCmd.AddCommand(receiver.CMDReceiver, sender.CMDSender)
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("receive error: %s", err)
	}

	log.Println("Canceling...")
}
