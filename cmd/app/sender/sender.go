package sender

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/farir1408/simple-kafka/pkg/types"

	"github.com/farir1408/simple-kafka/pkg/producer"
	"go.uber.org/zap"
)

var (
	brokers   []string
	topic     string
	CMDSender = &cobra.Command{
		Use:                   "sender --brokers=\"192.168.0.2:123,192.168.0.2:123\" --topic=topic_name",
		Short:                 "send message",
		Long:                  `send message to message-broker on "--brokers" hosts list.`,
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return StartSender(os.Stdin, brokers, topic)
		},
	}
)

func init() {
	CMDSender.Flags().StringSliceVarP(&brokers, "brokers", "b", []string{}, "brokers uris")
	CMDSender.MarkFlagRequired("brokers")
	CMDSender.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
	CMDSender.MarkFlagRequired("topic")
}

func StartSender(in io.Reader, uris []string, topic string) error {
	ctx := context.Background()
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	p, err := producer.New(uris, logger)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(in)
	logger.Info("Input messages...")
	for scanner.Scan() {
		msg := scanner.Bytes()
		if bytes.Equal(bytes.ToLower(msg), []byte("exit")) {
			return nil
		}
		p.SaveMessage(ctx, topic, types.Message{
			Ctx:  ctx,
			Body: msg,
		})
	}

	return scanner.Err()
}
