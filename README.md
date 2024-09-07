### a golang lib of subscribe
first, it is copy from https://github.com/jursonmo/subpub/tree/master/subscribe, here make it standalone

从 https://github.com/jursonmo/subpub/tree/master/subscribe 中拷贝过来，独立使用，方便后续维护、方便被其他项目引用。

### example:
```go
func main() {
	topic := "topic-test"

	subID1 := SubscriberID("subID-1")
	sub1 := NewSubscriber(subID1, func(topic string, d []byte) error {
		// todo: handle msg from topic
		fmt.Printf("sub1 receive topic:%s, msg:%s\n", topic, string(d))
		return nil
	})

	subID2 := SubscriberID("subID-2")
	sub2 := NewSubscriber(subID2, func(topic string, d []byte) error {
		// todo: handle msg from topic
		fmt.Printf("sub2 receive topic:%s, msg:%s\n", topic, string(d))
		return nil
	})

	err := sub1.Subscribe(topic)
	if err != nil {
		panic(err)
	}

	err = sub2.Subscribe(topic)
	if err != nil {
		panic(err)
	}

	err = sub1.Publish(topic, []byte("hello")) //sub2 will receive msg
	if err != nil {
		t.Fatal(err)
	}

	err = sub2.Publish(topic, []byte("hello")) // sub1 will receive msg
	if err != nil {
		panic(err)
	}

	sub1.Unsubscribe(topic) //sub1 will not receive msg from topic-test
    sub2.Unsubscribe(topic) //sub2 will not receive msg from topic-test
}
```