package system

import (
	"os"
	"os/signal"
)

// StopOnSignal returns a channel closed on the first of sigs (typically
// SIGTERM, SIGINT). Wire it to Dependencies.Shutdown.
func StopOnSignal(sigs ...os.Signal) <-chan struct{} {
	ch := make(chan struct{})
	c := make(chan os.Signal, 1)
	signal.Notify(c, sigs...)
	go func() {
		<-c
		signal.Stop(c)
		close(ch)
	}()
	return ch
}
