package receiver

import (
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/farir1408/simple-kafka/pkg/consumer"
	"go.uber.org/zap"
)

var (
	brokers     []string
	group       string
	topic       string
	CMDReceiver = &cobra.Command{
		Use:                   "receiver --brokers=\"192.168.0.2:123,192.168.0.2:123\" --consumer-group=group_name",
		Short:                 "start receive messages",
		Long:                  `receive messages from message-broker from "--brokers" hosts list.`,
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return StartReceiver(os.Stdout, brokers, group, topic)
		},
	}
)

func init() {
	CMDReceiver.Flags().StringSliceVarP(&brokers, "brokers", "b", []string{}, "brokers uris")
	CMDReceiver.MarkFlagRequired("brokers")
	CMDReceiver.Flags().StringVarP(&group, "consumer-group", "g", "", "consumer-group name")
	CMDReceiver.MarkFlagRequired("consumer-group")
	CMDReceiver.Flags().StringVarP(&topic, "topic", "t", "", "topic name")
	CMDReceiver.MarkFlagRequired("topic")
}

func StartReceiver(out io.Writer, uris []string, group, topic string) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	c, err := consumer.NewConsumer(uris, group, topic, logger)
	if err != nil {
		return err
	}
	c.Start()

	for msg := range c.GetMessages() {
		msg.StartCommitFunc()
		_, err := out.Write(append(msg.Body, []byte{'\n'}...))
		if err != nil {
			return err
		}
		msg.CommitFunc()
	}

	return nil
}
