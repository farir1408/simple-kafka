package types

import "context"

type Message struct {
	Ctx             context.Context // meta-info, like opentracing.Span.
	Body            []byte          // message content.
	CommitFunc      func()          // call if message processed successful.
	StartCommitFunc func()          // call if start prepare message.
}

type Consumer interface {
	GetMessages() <-chan Message // for external access to messages.
	Start()                      // start consuming.
	Close() error                // close all sessions.
}

type Producer interface {
}
