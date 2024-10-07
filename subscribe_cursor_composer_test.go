package subscribe

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	sm := NewSubscriberMgr()
	topic := "concurrent-test-topic"
	numSubscribers := 1000
	var wg sync.WaitGroup

	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			subID := SubscriberID(fmt.Sprintf("sub-%d", id))
			handler := func(topic string, d []byte) error { return nil }
			sub := sm.NewSubscriber(subID, handler)

			err := sub.Subscribe(topic)
			if err != nil {
				t.Errorf("Failed to subscribe: %v", err)
			}

			time.Sleep(time.Millisecond * 10) // Simulate some work

			sub.UnSubscribe(topic)
		}(i)
	}

	wg.Wait()

	if sm.TopicNum() != 0 {
		t.Errorf("Expected 0 topics after all unsubscriptions, got %d", sm.TopicNum())
	}
}

func TestPublishToMultipleTopics(t *testing.T) {
	sm := NewSubscriberMgr()
	topics := []string{"topic1", "topic2", "topic3"}
	message := "Hello, Multi-topic!"

	receivedMessages := make(map[string]int)
	var mu sync.Mutex

	handler := func(topic string, d []byte) error {
		mu.Lock()
		defer mu.Unlock()
		receivedMessages[topic]++
		return nil
	}

	for i := 0; i < 5; i++ {
		id := SubscriberID(fmt.Sprintf("subscriber-%d", i))
		sub := sm.NewSubscriber(id, handler)
		for _, topic := range topics {
			err := sub.Subscribe(topic)
			if err != nil {
				t.Fatalf("Failed to subscribe: %v", err)
			}
		}
	}

	for _, topic := range topics {
		n, err := sm.Publish(nil, topic, []byte(message))
		if err != nil {
			t.Fatalf("Failed to publish to topic %s: %v", topic, err)
		}
		if n != 5 {
			t.Fatalf("Expected 5 messages to be published to topic %s, got %d", topic, n)
		}
	}

	time.Sleep(time.Millisecond * 50)

	for _, topic := range topics {
		if count := receivedMessages[topic]; count != 5 {
			t.Errorf("Expected 5 messages for topic %s, got %d", topic, count)
		}
	}
}

func TestPublishFromSubID(t *testing.T) {
	sm := NewSubscriberMgr()
	topic := "test-topic"
	message := "Hello from SubID"

	// Create two subscribers
	sub1ID := SubscriberID("subscriber-1")
	sub2ID := SubscriberID("subscriber-2")

	receivedMessages := make(map[SubscriberID]string)
	var mu sync.Mutex

	handler1 := func(rcvTopic string, d []byte) error {
		mu.Lock()
		defer mu.Unlock()
		receivedMessages[sub1ID] = string(d)
		return nil
	}

	handler2 := func(rcvTopic string, d []byte) error {
		mu.Lock()
		defer mu.Unlock()
		receivedMessages[sub2ID] = string(d)
		return nil
	}

	sub1 := sm.NewSubscriber(sub1ID, handler1)
	sub2 := sm.NewSubscriber(sub2ID, handler2)

	// Subscribe both to the topic
	err := sub1.Subscribe(topic)
	if err != nil {
		t.Fatalf("Failed to subscribe sub1: %v", err)
	}
	err = sub2.Subscribe(topic)
	if err != nil {
		t.Fatalf("Failed to subscribe sub2: %v", err)
	}

	// Publish from sub1ID
	n, err := sm.PublishFromSubID(sub1ID, topic, []byte(message))
	if err != nil {
		t.Fatalf("Failed to publish from sub1ID: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 message to be published, got %d", n)
	}

	// Wait a bit for the message to be processed
	time.Sleep(10 * time.Millisecond)

	// Check that only sub2 received the message
	if msg, ok := receivedMessages[sub2ID]; !ok || msg != message {
		t.Errorf("Expected sub2 to receive message '%s', got '%s'", message, msg)
	}
	if _, ok := receivedMessages[sub1ID]; ok {
		t.Errorf("sub1 should not have received any message")
	}

	// Clear received messages
	receivedMessages = make(map[SubscriberID]string)

	// Publish from sub2ID
	n, err = sm.PublishFromSubID(sub2ID, topic, []byte(message))
	if err != nil {
		t.Fatalf("Failed to publish from sub2ID: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 message to be published, got %d", n)
	}

	// Wait a bit for the message to be processed
	time.Sleep(10 * time.Millisecond)

	// Check that only sub1 received the message
	if msg, ok := receivedMessages[sub1ID]; !ok || msg != message {
		t.Errorf("Expected sub1 to receive message '%s', got '%s'", message, msg)
	}
	if _, ok := receivedMessages[sub2ID]; ok {
		t.Errorf("sub2 should not have received any message")
	}

	// Test publishing from non-existent SubID
	nonExistentID := SubscriberID("non-existent")
	_, err = sm.PublishFromSubID(nonExistentID, topic, []byte(message))
	if err == nil {
		t.Errorf("Expected error when publishing from non-existent SubID, got nil")
	}
}

func TestSubscriberMgrWithChecks(t *testing.T) {
	subscribeCheck := func(from Subscriber, topic string) error {
		if from.Id() == "blocked" {
			return fmt.Errorf("subscriber is blocked")
		}
		return nil
	}

	publishCheck := func(from Subscriber, topic string, data []byte) error {
		if string(data) == "blocked message" {
			return fmt.Errorf("message content is blocked")
		}
		return nil
	}

	sm := NewSubscriberMgr(WithSubscribeCheck(subscribeCheck), WithPublishCheck(publishCheck))

	// Test subscribe check
	blockedSub := sm.NewSubscriber("blocked", func(topic string, d []byte) error { return nil })
	err := blockedSub.Subscribe("test-topic")
	if err == nil || err.Error() != "subscriber is blocked" {
		t.Errorf("Expected 'subscriber is blocked' error, got: %v", err)
	}

	// Test publish check
	allowedSub := sm.NewSubscriber("allowed", func(topic string, d []byte) error { return nil })
	err = allowedSub.Subscribe("test-topic")
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	_, err = sm.Publish(nil, "test-topic", []byte("blocked message"))
	if err == nil || err.Error() != "message content is blocked" {
		t.Errorf("Expected 'message content is blocked' error, got: %v", err)
	}

	n, err := sm.Publish(nil, "test-topic", []byte("allowed message"))
	if err != nil {
		t.Errorf("Unexpected error when publishing allowed message: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected 1 message to be published, got %d", n)
	}
}

func TestTopicWatcherMultipleEvents(t *testing.T) {
	sm := NewSubscriberMgr()
	watcherId := "multi-event-watcher"

	watcher, err := sm.NewTopicsWatcher(watcherId)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	topics := []string{"topic1", "topic2", "topic3"}
	expectedEvents := []WatchTopicEvent{}

	for _, topic := range topics {
		expectedEvents = append(expectedEvents,
			WatchTopicEvent{Op: TopicAdd, Topic: topic},
			WatchTopicEvent{Op: TopicDel, Topic: topic})
	}

	go func() {
		for _, topic := range topics {
			id := SubscriberID(fmt.Sprintf("test-subscriber-%s", topic))
			handler := func(topic string, d []byte) error { return nil }
			sub := sm.NewSubscriber(id, handler)

			sub.Subscribe(topic)
			time.Sleep(time.Millisecond * 10)
			sub.UnSubscribe(topic)
			time.Sleep(time.Millisecond * 10)
		}
	}()

	receivedEvents := []WatchTopicEvent{}
	timeout := time.After(time.Second)

eventLoop:
	for {
		select {
		case event := <-watcher.Event():
			receivedEvents = append(receivedEvents, event)
			if len(receivedEvents) == len(expectedEvents) {
				break eventLoop
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for events. Received %d out of %d expected events",
				len(receivedEvents), len(expectedEvents))
		}
	}

	if len(receivedEvents) != len(expectedEvents) {
		t.Fatalf("Expected %d events, got %d", len(expectedEvents), len(receivedEvents))
	}

	for i, expected := range expectedEvents {
		if receivedEvents[i] != expected {
			t.Errorf("Event %d: expected %v, got %v", i, expected, receivedEvents[i])
		}
	}

	err = watcher.Stop()
	if err != nil {
		t.Fatalf("Failed to stop watcher: %v", err)
	}
}
