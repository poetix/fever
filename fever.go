package main

import (
	"fmt"
	. "sync"
	"time"
	. "time"
)

type EventTarget struct {
	AggregateType string
	AggregateId   string
}

func (target EventTarget) String() string {
	return fmt.Sprintf("%s:%s", target.AggregateType, target.AggregateId)
}

type EventTimestamp struct {
	OccurredMs int64
	StreamId   string
}

func (timestamp EventTimestamp) String() string {
	return fmt.Sprintf("%s/%s", time.Unix(timestamp.OccurredMs, 0), timestamp.StreamId)
}

type VersionedName struct {
	Name    string
	Version string
}

func (versionedName VersionedName) String() string {
	return fmt.Sprintf("%s_%s", versionedName.Name, versionedName.Version)
}

type EventData map[string]string

type Event struct {
	EventTarget
	EventTimestamp
	EventType VersionedName
	EventData
}

func (event Event) String() string {
	return fmt.Sprintf("%s %s at %s %s",
		event.EventTarget,
		event.EventType,
		event.EventTimestamp,
		event.EventData)
}

type EventStore struct {
	wg       WaitGroup
	incoming chan eventStoreWriteRequest
}

func (e *EventStore) Start() {
	e.incoming = make(chan eventStoreWriteRequest, 100)
	e.wg.Add(1)
	go e.main()
}

func (e *EventStore) Close() {
	close(e.incoming)
	e.wg.Wait()
}

type eventStoreWriteRequest struct {
	*Event
	ack chan Time
}

func (e *EventStore) SubmitAsync(evt *Event) <-chan Time {
	ack := make(chan Time)
	e.incoming <- eventStoreWriteRequest{evt, ack}
	return ack
}

func (e *EventStore) Submit(evt *Event) Time {
	ack := e.SubmitAsync(evt)
	completed := <-ack
	return completed
}

func (e *EventStore) main() {
	for writeRequest := range e.incoming {
		fmt.Println(writeRequest.Event)
		writeRequest.ack <- time.Now()
		close(writeRequest.ack)
	}
	e.wg.Done()
}

func main() {
	store := EventStore{}
	store.Start()

	ack1 := store.SubmitAsync(&Event{
		EventTarget:    EventTarget{"test", "test-id1"},
		EventTimestamp: EventTimestamp{time.Now().Unix(), "test"},
		EventType:      VersionedName{"created", "0.0.1"},
		EventData:      EventData{"foo": "bar"}})

	ack2 := store.SubmitAsync(&Event{
		EventTarget:    EventTarget{"test", "test-id2"},
		EventTimestamp: EventTimestamp{time.Now().Unix(), "test"},
		EventType:      VersionedName{"created", "0.0.1"},
		EventData:      EventData{"baz": "xyzzy"}})

	time1 := <-ack1
	time2 := <-ack2

	fmt.Println(time1)
	fmt.Println(time2)

	store.Close()
}
