package consumer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/farir1408/simple-kafka/pkg/types"
)

// Sarama `ConsumerGroupHandler` implementation.
type Handler struct {
	name       string
	wg         *sync.WaitGroup
	msgChannel chan types.Message
}

func NewHandler(name string) *Handler {
	h := &Handler{name: name}
	h.wg = &sync.WaitGroup{}
	// The number of messages that can be read during a pause.
	h.msgChannel = make(chan types.Message)
	return h
}

// Close ...
func (h *Handler) Close() error {
	close(h.msgChannel)
	return nil
}

// Setup - call before consume.
func (h *Handler) Setup(cgSession sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup call before session is closed and after consume.
func (h *Handler) Cleanup(cgSession sarama.ConsumerGroupSession) error {
	// Lock to wait for commit all messages.
	fmt.Println("start wait for commit")
	h.wg.Wait()
	fmt.Println("committed")
	return nil
}

// ConsumeClaim start consume.
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, cgClaim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for {
		select {
		case msg, ok := <-cgClaim.Messages():
			if !ok {
				fmt.Println("read channel closed ")
				return errors.New("channel closed")
			}
			//TODO: сделать автокомит сообщений при чтении из кафки.
			select {
			case h.msgChannel <- types.Message{Ctx: ctx, Body: msg.Value, CommitFunc: h.makeCommitFunc(msg, session), StartCommitFunc: h.makeStartCommitFunc()}:
			case <-ctx.Done():
				return errors.New("session is done")
			}
		case <-ctx.Done():
			return errors.New("session is done")
		}
	}
}

func (h *Handler) makeStartCommitFunc() func() {
	// If `wg.Add` is placed in `makeCommitFunc`, then when writing the `types.Message` in to the channel `h.msgChannel`,
	// you can block, and the `wg.Add` will be added.
	// In this case, if you do not read this message from the channel, and the session tries to close,
	// then it will not be closed and will wait until the receipt of a message is confirmed, which, in fact, was not written to the channel.
	return func() {
		h.wg.Add(1)
	}
}

func (h *Handler) makeCommitFunc(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) func() {
	return func() {
		defer h.wg.Done()
		session.MarkMessage(msg, "")
	}
}

func (h *Handler) GetMessages() <-chan types.Message {
	return h.msgChannel
}
