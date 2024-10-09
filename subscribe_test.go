package subscribe

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// go test -v -race
// go test -bench="." -benchmem
// go test -bench="BenchmarkSubscribe" -benchmem -memprofile="out.profile"
// go tool pprof out.profile
// 测试时间默认是1秒，也就是1秒的时间。如果想让测试运行的时间更长，可以通过 -benchtime= 指定
// go test -bench="BenchmarkSubscribe" -benchmem -memprofile="out.profile" -benchtime=3s
// -benchtime 的值除了是时间外，还可以是具体的次数。例如，执行 30 次可以用 -benchtime=30x：
// BenchmarkFib-8 中的 -8 即 GOMAXPROCS，默认等于 CPU 核数。
// 可以通过 -cpu 参数改变 GOMAXPROCS，-cpu 支持传入一个列表作为参数，例如：
// go test -bench="." -cpu=2,4, 意思分别用2个核执行测试，用 4 个核执行测试。并不是用第二个、第四个核执行测试
func sliceContains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

var Topic1 = "topic1-test"
var Topic2 = "topic2-test"
var Topic3 = "topic3-test"

func TestBaseFunc(t *testing.T) {
	sm := NewSubscriberMgr()
	subID1 := "subID-1"
	sub1ReceiveMsg := ""
	sub1ReceiveTopic := ""
	sub1 := sm.NewSubscriber(SubscriberID(subID1), func(topic string, b []byte) error {
		t.Logf("%s receive topic:%s, msg:%s", subID1, topic, string(b))
		sub1ReceiveTopic = topic
		sub1ReceiveMsg = string(b)
		return nil
	})

	subID2 := "subID-2"
	sub2ReceiveMsg := ""
	sub2ReceiveTopic := ""
	sub2 := sm.NewSubscriber(SubscriberID(subID2), func(topic string, b []byte) error {
		t.Logf("%s receive topic:%s, msg:%s", subID2, topic, string(b))
		sub2ReceiveTopic = topic
		sub2ReceiveMsg = string(b)
		return nil
	})

	//1. 测试订阅topic
	sub1.Subscribe(Topic1)
	if !reflect.DeepEqual(sub1.Topics(), []string{Topic1}) {
		t.Fatalf("sub1.Topics():%v, topics:%v", sub1.Topics(), []string{Topic1})
	}

	//2. 测试sub2 publish 消息给 sub1
	sub2PublishMsg := "message from sub2"
	sub2.Publish(Topic1, []byte(sub2PublishMsg)) //公告消息， sub1 会收到消息，并保存在sub1ReceiveMsg

	time.Sleep(time.Millisecond * 20)

	if sub1ReceiveTopic != Topic1 {
		t.Fatalf("sub1ReceiveTopic:%s should be Topic1:%s", sub1ReceiveTopic, Topic1)
	}
	if sub1ReceiveMsg != sub2PublishMsg {
		t.Fatalf("sub1ReceiveMsg:%s, sub2PublishMsg:%s", sub1ReceiveMsg, sub2PublishMsg)
	}

	//3. 测试sm publish 消息给 sub1, sub2, 看多个subscriber 能不能收到相同消息
	sub2.Subscribe(Topic1)
	smPublishMsg := "message from sm"
	n, err := sm.Publish(nil, Topic1, []byte(smPublishMsg))
	if err != nil {
		t.Fatal(err)
	}
	//发给两个订阅者
	if n != 2 {
		t.Fatalf("n:%d should be 2,", n)
	}
	time.Sleep(time.Millisecond * 20)
	if sub1ReceiveMsg != smPublishMsg {
		t.Fatalf("sub1ReceiveMsg:%s, smPublishMsg:%s", sub1ReceiveMsg, smPublishMsg)
	}
	if sub2ReceiveMsg != smPublishMsg {
		t.Fatalf("sub2ReceiveMsg:%s, smPublishMsg:%s", sub2ReceiveMsg, smPublishMsg)
	}
	if sub2ReceiveTopic != Topic1 {
		t.Fatalf("sub2ReceiveTopic:%s should be Topic1:%s", sub2ReceiveTopic, Topic1)
	}

	//4. 测试sub1退订后，不再接受到指定的消息, 只有sub2收到消息
	sub1.UnSubscribe(Topic1)
	if sliceContains(sub1.Topics(), Topic1) {
		t.Fatalf("sub1.Topics():%v, shouldn't contains %s", sub1.Topics(), Topic1)
	}

	smPublishMsg2 := "message2 from sm"
	n, err = sm.Publish(nil, Topic1, []byte(smPublishMsg2))
	if err != nil {
		t.Fatal(err)
	}
	//发给订阅者的数量应该是1, 只剩下sub2一个订阅者
	if n != 1 {
		t.Fatalf("n:%d should be 1,", n)
	}

	time.Sleep(time.Millisecond * 20)

	//sub1 还是原来的消息
	if sub1ReceiveMsg != smPublishMsg {
		t.Fatalf("sub1ReceiveMsg:%s, smPublishMsg:%s", sub1ReceiveMsg, smPublishMsg)
	}
	//sub2 收到新的smPublishMsg2消息
	if sub2ReceiveMsg != smPublishMsg2 {
		t.Fatalf("sub2ReceiveMsg:%s, smPublishMsg2:%s", sub2ReceiveMsg, smPublishMsg2)
	}

	//5. sub1再次订阅Topic1, 是否又能接受到消息， 当前应该又有两个订阅者
	sub1.Subscribe(Topic1)
	msg := "message3 from sm"
	n, err = sm.Publish(nil, Topic1, []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	//发给订阅者的数量应该是2
	if n != 2 {
		t.Fatalf("n:%d should be 2,", n)
	}

	if sub1ReceiveMsg != msg {
		t.Fatalf("sub1ReceiveMsg:%s, smPublishMsg:%s", sub1ReceiveMsg, msg)
	}
	if sub2ReceiveMsg != msg {
		t.Fatalf("sub2ReceiveMsg:%s, smPublishMsg:%s", sub2ReceiveMsg, msg)
	}

	//6.测试订阅者订阅多个主题的情况下，能否接受到不同主题的消息
	type Msg struct {
		topic string
		msg   string
	}
	testMsgs := []Msg{{Topic1, "Topic1 msg"}, {Topic2, "Topic2 msg"}, {Topic3, "Topic3 msg"}}
	subID3 := "subID-3"
	sub3ReceiveMsgs := []Msg{}
	sub3 := sm.NewSubscriber(SubscriberID(subID3), func(topic string, b []byte) error {
		t.Logf("%s receive topic:%s, msg:%s", subID3, topic, string(b))
		sub3ReceiveMsgs = append(sub3ReceiveMsgs, Msg{topic: topic, msg: string(b)})
		return nil
	})
	sub3.Subscribe(Topic1)
	sub3.Subscribe(Topic2)
	sub3.Subscribe(Topic3)
	//公告三个不同topic的消息
	for _, msg := range testMsgs {
		sm.Publish(nil, msg.topic, []byte(msg.msg))
	}
	sm.Publish(nil, "topic-x", []byte("topic-x msg")) //发送一个sub3 没有订阅的topic 的消息
	time.Sleep(time.Millisecond * 20)

	//检查sub3是否完整接受已订阅的主题消息，
	if !reflect.DeepEqual(sub3ReceiveMsgs, testMsgs) {
		t.Fatalf("sub3ReceiveMsgs:%+v, testMsgs:%+v", sub3ReceiveMsgs, testMsgs)
	}

	//7. 测试sub3 Close(), 即退订所有topic, 应该接受不到任何新的消息
	sub3ReceiveMsgs = []Msg{} //重置
	sub3.Close()
	if len(sub3.Topics()) != 0 {
		t.Fatalf("after close, len(sub3.Topics()):%d should be 0", len(sub3.Topics()))
	}

	for _, msg := range testMsgs {
		sm.Publish(nil, msg.topic, []byte(msg.msg))
	}
	time.Sleep(time.Millisecond * 20)
	if len(sub3ReceiveMsgs) != 0 {
		t.Fatalf("sub3ReceiveMsgs:%+v, should empty", sub3ReceiveMsgs)
	}

}

func TestRepeatSubscribe(t *testing.T) {
	subID1 := SubscriberID("subID-1")
	//sm := NewSubscriberMgr()
	//sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })
	sub1 := NewSubscriber(subID1, func(topic string, d []byte) error { return nil })

	err := sub1.Subscribe(Topic1)
	if err != nil {
		t.Fatal(err)
	}
	err = sub1.Subscribe(Topic1)
	if err == nil {
		t.Fatalf("expect err!= nil, but got nil")
	}
}

func TestSubscriberClose(t *testing.T) {
	sm := NewSubscriberMgr()
	subID1 := SubscriberID("subID-1")
	topic := "topic-test"
	sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })

	err := sub1.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(sub1.Topics()) != 1 {
		t.Fatalf("after Subscribe, expect len(sub1.Topics()=1, but got %d", len(sub1.Topics()))
	}
	//测试获取订阅topic的数量
	if sm.TopicNum() != 1 {
		t.Fatalf("expect sm.TopicNum():1, but got %d", sm.TopicNum())
	}
	//测试根据topic 获取 订阅者数量
	if n := len(sm.GetSubscribers(topic)); n != 1 {
		t.Fatalf("expect len(sm.GetSubscribers(topic)):1, but got %d", n)
	}

	sub1.Close()

	if len(sub1.Topics()) != 0 {
		t.Fatalf("after close, len(sub3.Topics()):%d should be 0", len(sub1.Topics()))
	}

	//测试获取订阅topic的数量
	if sm.TopicNum() != 0 {
		t.Fatalf("expect sm.TopicNum():0, but got %d", sm.TopicNum())
	}

	//测试根据topic 获取订阅者数量
	if n := len(sm.GetSubscribers(topic)); n != 0 {
		t.Fatalf("expect len(sm.GetSubscribers(topic)):0, but got %d", n)
	}
}

func TestAddSubscribeCheck(t *testing.T) {
	subID1 := SubscriberID("subID-1")
	subID2 := SubscriberID("subID-2")

	subID1InvaildTopic := "invaild-topic"
	vaildTopic := "vaild-topic"

	//订阅者订阅主题时的回调函数
	check := func(from Subscriber, topic string) error {
		//subID1 不允许订阅subID1InvaildTopic 主题
		if from.Id() == subID1 && topic == subID1InvaildTopic {
			return fmt.Errorf("%s is not allowd to subscribe this topic", from.Id())
		}
		return nil
	}

	sm := NewSubscriberMgr(WithSubscribeCheck(check))
	sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })
	sub2 := sm.NewSubscriber(subID2, func(topic string, d []byte) error { return nil })

	err := sub1.Subscribe(vaildTopic)
	if err != nil {
		t.Fatal(err)
	}

	//sub1订阅 subID1InvaildTopic，应该不成功
	err = sub1.Subscribe(subID1InvaildTopic)
	if err == nil {
		t.Fatalf("%s shouldn't Subscribe subID1InvaildTopic:%s", sub1.Id(), subID1InvaildTopic)
	}

	//sub2 应该可以订阅 subID1InvaildTopic，
	err = sub2.Subscribe(subID1InvaildTopic)
	if err != nil {
		t.Fatalf("%s should Subscribe subID1InvaildTopic:%s", sub2.Id(), subID1InvaildTopic)
	}
}

func TestPublishCheck(t *testing.T) {
	subID1 := SubscriberID("subID-1")
	subID2 := SubscriberID("subID-2")

	subID1InvaildTopic := "invaild-topic"
	vaildTopic := "vaild-topic"

	publishCheck := func(from Subscriber, topic string, data []byte) error {
		//不允许 subID1 发布 subID1InvaildTopic 主题的消息
		if from.Id() == subID1 && topic == subID1InvaildTopic {
			return fmt.Errorf("%s is not allowd to publish this topic", from.Id())
		}
		return nil
	}

	sm := NewSubscriberMgr(WithPublishCheck(publishCheck))
	sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })
	sub2ReceiveMsg := ""
	sub2 := sm.NewSubscriber(subID2, func(topic string, d []byte) error { sub2ReceiveMsg = string(d); return nil })

	//sub2 订阅了所有主题
	err := sub2.Subscribe(subID1InvaildTopic)
	if err != nil {
		t.Fatal(err)
	}
	err = sub2.Subscribe(vaildTopic)
	if err != nil {
		t.Fatal(err)
	}

	//sub1 发布subID1InvaildTopic 主题的消息，应该不成功
	err = sub1.Publish(subID1InvaildTopic, []byte("subID1InvaildTopic msg"))
	if err == nil {
		t.Fatalf("%s shouldn't allowed to publish %s msg", sub1.Id(), subID1InvaildTopic)
	}

	//sub1 发布 vaildTopic 主题的消息，应该成功
	msg := "vaildTopic msg"
	err = sub1.Publish(vaildTopic, []byte(msg))
	if err != nil {
		t.Fatalf("%s should allowed to publish %s msg", sub1.Id(), subID1InvaildTopic)
	}
	time.Sleep(time.Millisecond * 10)
	//sub2 应该能收到 sub1 公告的msg
	if sub2ReceiveMsg != msg {
		t.Fatalf("sub2ReceiveMsg:%s should be same as  msg:%s ", sub2ReceiveMsg, msg)
	}
}

// marsCode 生成的代码
func TestNewTopicsWatcher(t *testing.T) {
	sm := NewSubscriberMgr()

	// 测试正常创建新的 TopicWatcher
	watcher, err := sm.NewTopicsWatcher("testWatchId")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if watcher == nil {
		t.Errorf("Expected watcher to be not nil")
	}

	// 测试 watchId 已存在的情况
	_, err = sm.NewTopicsWatcher("testWatchId")
	if err == nil {
		t.Errorf("Expected error for existing watchId, got nil")
	} else if err.Error() != "watchId:testWatchId already exists" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

func TestTopicsWatcher(t *testing.T) {
	topic := "topic-test"

	subID1 := SubscriberID("subID-1")
	subID2 := SubscriberID("subID-2")

	sm := NewSubscriberMgr()
	watcher, err := sm.NewTopicsWatcher("watcher-1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = sm.NewTopicsWatcher("watcher-1")
	if err == nil {
		t.Fatal("watcher-1 have exist, repeat create watcher-1, expect err != nil, but got nil")
	}

	sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })
	sub2 := sm.NewSubscriber(subID2, func(topic string, d []byte) error { return nil })

	err = sub1.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}
	//第一次订阅topic, 应该收到TopicAdd事件
	select {
	case <-time.After(time.Millisecond * 10):
		t.Fatal("error: timeout for TopicAdd")
	case event := <-watcher.Event():
		if event.Op != TopicAdd || event.Topic != topic {
			t.Fatalf("event.Op:%s, event.Topic:%s expect op:TopicAdd, topic:%s", event.Op, event.Topic, topic)
		}
		break
	}

	err = sub2.Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	//第二次订阅topic, topic 已经存在, 应该收到不到TopicAdd事件
	time.Sleep(time.Millisecond * 50)
	if len(watcher.Event()) != 0 {
		t.Fatalf("expect len(ch):0, but got %d", len(watcher.Event()))
	}

	sub1.UnSubscribe(topic) //sub1 退订topic, 应该收到不到任何事件，因为sub2 还在订阅topic
	time.Sleep(time.Millisecond * 50)
	if len(watcher.Event()) != 0 {
		t.Fatalf("expect len(ch):0, but got %d", len(watcher.Event()))
	}

	sub2.UnSubscribe(topic)
	//sub2 退订topic, 应该收到TopicDel事件, 因为sub2 是最后一个订阅topic的订阅者
	select {
	case <-time.After(time.Millisecond * 10):
		t.Fatal("error: timeout for TopicDel")
	case event := <-watcher.Event():
		if event.Op != TopicDel || event.Topic != topic {
			t.Fatalf("event.Op:%s, event.Topic:%s expect op:TopicDel, topic:%s", event.Op, event.Topic, topic)
		}
		break
	}

	//所有订阅者都退订了topic, topicNum 应该为0
	if sm.TopicNum() != 0 {
		t.Fatalf("expect sm.TopicNum():0, but got %d", sm.TopicNum())
	}

	//删除watcher后，watcher的Event()应该是关闭的
	sm.DelTopicsWatcher("watcher-1")
	_, isOpen := <-watcher.Event()
	if isOpen {
		t.Fatalf("expect watcher event channel is closed, but result is open")
	}
}

func TestTopicWatcherStop(t *testing.T) {
	sm := NewSubscriberMgr()

	// Create a new watcher
	watcherId := "test-watcher"
	watcher, err := sm.NewTopicsWatcher(watcherId)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	// Stop the watcher
	err = watcher.Stop()
	if err != nil {
		t.Fatalf("Failed to stop watcher: %v", err)
	}

	// Try to read from the event channel, it should be closed
	select {
	case _, ok := <-watcher.Event():
		if ok {
			t.Fatalf("Event channel is still open after stopping")
		}
	default:
		t.Fatalf("Event channel should be closed, but it's not")
	}

	// Try to stop the watcher again, it should return an error
	err = watcher.Stop()
	if err == nil {
		t.Fatalf("Stopping an already stopped watcher should return an error")
	}
}

func BenchmarkSubscribe(b *testing.B) {
	sm := NewSubscriberMgr()
	subID1 := SubscriberID("subID-1")
	sub1 := sm.NewSubscriber(subID1, func(topic string, d []byte) error { return nil })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := sub1.Subscribe("topic-test")
		if err != nil {
			b.Fatal(err)
		}
		sub1.UnSubscribe("topic-test")
	}
	b.StopTimer()

	if sm.TopicNum() != 0 {
		b.Fatalf("expect sm.TopicNum():0, but got %d", sm.TopicNum())
	}

	if len(sub1.Topics()) != 0 {
		b.Fatalf("expect len(sub1.Topics()):0, but got %d", len(sub1.Topics()))
	}

}

/*
go test -bench="." -benchmem
goos: darwin
goarch: arm64
pkg: github.com/jursonmo/subscribe
BenchmarkSubscribe-8   	 4825744	       223.4 ns/op	     416 B/op	       4 allocs/op
PASS
ok  	github.com/jursonmo/subscribe	1.976s
*/

func BenchmarkConcurrentSubscribe(b *testing.B) {
	sm := NewSubscriberMgr()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		subID := SubscriberID(fmt.Sprintf("subID-%d", time.Now().UnixNano()))
		sub := sm.NewSubscriber(subID, func(topic string, d []byte) error { return nil })
		topic := fmt.Sprintf("topic-%d", time.Now().UnixNano())
		//如果有两个Parallel goroutine 生成相同的subID, topic(这种情况有一定概率发生), 会panic(已经修复)
		fmt.Println("subID:", subID, "topic:", topic)

		for pb.Next() {
			err := sub.Subscribe(topic)
			if err != nil {
				b.Fatal(err)
			}
			sub.UnSubscribe(topic)
		}
	})
	b.StopTimer()

	if sm.TopicNum() != 0 {
		b.Fatalf("expect sm.TopicNum():0, but got %d", sm.TopicNum())
	}
}

/*
go test -bench=BenchmarkConcurrentSubscribe -cpu=2,4 -run=^$ -benchmem -benchtime=10s
will@willdebijibendiannao subscribe % go test -bench=BenchmarkConcurrentSubscribe -cpu=1,2 -run=^$
subID: subID-1728491247168083000 topic: topic-1728491247168128000
subID: subID-1728491247168113000 topic: topic-1728491247168144000
goos: darwin
goarch: arm64
pkg: github.com/jursonmo/subscribe
BenchmarkConcurrentSubscribe
subID: subID-1728491247168539000 topic: topic-1728491247168552000
subID: subID-1728491247168963000 topic: topic-1728491247168979000
subID: subID-1728491247176032000 topic: topic-1728491247176041000
subID: subID-1728491247486902000 topic: topic-1728491247486910000
 3862513               292.9 ns/op
BenchmarkConcurrentSubscribe-2
subID: subID-1728491248618516000 topic: topic-1728491248618524000
subID: subID-1728491248618518000 topic: topic-1728491248618526000
subID: subID-1728491248618761000 topic: topic-1728491248618766000
subID: subID-1728491248618768000 topic: topic-1728491248618769000
subID: subID-1728491248618995000 topic: topic-1728491248618999000
subID: subID-1728491248618998000 topic: topic-1728491248619003000
subID: subID-1728491248623195000 topic: topic-1728491248623200000
subID: subID-1728491248623199000 topic: topic-1728491248623219000
subID: subID-1728491249027250000 topic: topic-1728491249027260000
subID: subID-1728491249027255000 topic: topic-1728491249027273000
 2971753               365.6 ns/op
PASS
ok      github.com/jursonmo/subscribe   4.020s
*/

// 为了避免有两个Parallel goroutine 生成相同的subID, topic，用一个递增id来生成subID, topic
func BenchmarkConcurrentSubscribev2(b *testing.B) {
	sm := NewSubscriberMgr()
	id := int64(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := atomic.AddInt64(&id, 1)
		subID := SubscriberID(fmt.Sprintf("subID-%d", idx))
		sub := sm.NewSubscriber(subID, func(topic string, d []byte) error { return nil })
		topic := fmt.Sprintf("topic-%d", idx)

		n := 0
		for pb.Next() {
			err := sub.Subscribe(topic)
			if err != nil {
				b.Fatal(err)
			}
			sub.UnSubscribe(topic)
			n++
		}
		fmt.Println("subID:", subID, "topic:", topic, "n:", n)
	})
	b.StopTimer()

	if sm.TopicNum() != 0 {
		b.Fatalf("expect sm.TopicNum():0, but got %d", sm.TopicNum())
	}
}

/*
will@willdebijibendiannao subscribe % go test -bench=BenchmarkConcurrentSubscribev2 -cpu=1,2 -run=^$
goos: darwin
goarch: arm64
pkg: github.com/jursonmo/subscribe
BenchmarkConcurrentSubscribev2
subID: subID-2 topic: topic-2 n: 0
subID: subID-1 topic: topic-1 n: 1
subID: subID-1 topic: topic-1 n: 100
subID: subID-1 topic: topic-1 n: 10000
subID: subID-1 topic: topic-1 n: 1000000
subID: subID-1 topic: topic-1 n: 3826284 //这里可以看到-cpu=1时, 只有一个goroutine执行测试，topic-2是没有得到测试的。
 3826284               299.2 ns/op
BenchmarkConcurrentSubscribev2-2
subID: subID-1 topic: topic-1 n: 1
subID: subID-2 topic: topic-2 n: 0
subID: subID-2 topic: topic-2 n: 16
subID: subID-1 topic: topic-1 n: 84
subID: subID-1 topic: topic-1 n: 4950
subID: subID-2 topic: topic-2 n: 5050
subID: subID-2 topic: topic-2 n: 500536
subID: subID-1 topic: topic-1 n: 499464
subID: subID-1 topic: topic-1 n: 1601321
subID: subID-2 topic: topic-2 n: 1593340
 3194661               379.4 ns/op
PASS
ok      github.com/jursonmo/subscribe   4.321s
will@willdebijibendiannao subscribe %
*/
// b.RunParallel 的 goroutine 是并发执行的，执行的goroutine数量是-cpu指定的数量, 如果-cpu=1 那么只会有一个goroutine执行。
// 执行BenchmarkConcurrentSubscribev2的次数是动态的. for pb.Next() 的次数是动态递增的.
