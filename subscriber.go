package subscribe

import (
	"fmt"
	"sync"
)

type MailBoxHandler func(topic string, d []byte) error

// client 订阅topic, 退出订阅， Put
type SubscriberID string
type Subscriber interface {
	Id() SubscriberID
	Subscribe(topic string) error
	UnSubscribe(topic string)
	Publish(topic string, data []byte) error    //订阅者可以发布消息？直接调用SubscriberMgr.Publish()来发布?
	MailBoxMsg(topic string, data []byte) error //非阻塞处理订阅的消息, 非阻塞，否则public是，会被某个订阅者拖慢
	Topics() []string                           //订阅了哪些topic
	Close() error                               //退订所有的topic
}

type Subscribe struct {
	sync.RWMutex
	sm      *SubscriberMgr
	id      SubscriberID
	topics  []string
	handler MailBoxHandler
	closed  bool //订阅主题和关闭时，需要判断是否已经closed. 退订时可以不用判断，因为那时的s.topics是空的
}

func (s *Subscribe) Id() SubscriberID {
	return s.id
}

func (s *Subscribe) MailBoxMsg(topic string, data []byte) error {
	if s.handler == nil {
		return fmt.Errorf("%v MailBox handler == nil", s.id)
	}
	return s.handler(topic, data)
}

func (s *Subscribe) Publish(topic string, data []byte) error {
	if s.sm == nil {
		return fmt.Errorf("%v sm == nil", s.id)
	}
	_, err := s.sm.Publish(s, topic, data)
	return err
}

func (s *Subscribe) Topics() []string {
	s.RLock()
	defer s.RUnlock()
	return s.topics
}

func (s *Subscribe) Subscribe(topic string) error {
	s.Lock()
	if s.isClosed() {
		s.Unlock()
		return fmt.Errorf("subscriber:%v already closed", s.id)
	}

	exist := false
	for _, t := range s.topics {
		if t == topic {
			exist = true
			break
		}
	}
	if !exist {
		s.topics = append(s.topics, topic)
	}
	s.Unlock()

	if !exist {
		return s.sm.AddSubscriber(topic, s)
	}
	return fmt.Errorf("subscriber:%v already Subscribed topic:%s", s.id, topic)
}

// 锁流程: 退订的锁---->SubscriberMgr{} 锁---> Subscribers{} 锁，注意不要死锁。
// UnSubscribe退订的时候，锁的颗粒度大一点，它包括了s.sm.RemoveSubscriber(s, topic)
// 但是没啥影响, 因为如果sm.RemoveSubscriber()hold锁的时间比较长,
// subscriber订阅时也比较长, 不会因为UnSubscribe hold锁的时间变短了而变短, 因为订阅需要调用sm.AddSubscriber()
// 本身订阅和退订的操作不是很频繁的事，对性能不敏感, 不是处理消息。
func (s *Subscribe) UnSubscribe(topic string) {
	s.Lock()
	defer s.Unlock()
	s.unSubscribe(topic)
}

// 小写开头，不加锁
func (s *Subscribe) unSubscribe(topic string) {
	removeIndex := 0
	exist := false
	for i, t := range s.topics {
		if t == topic {
			exist = true
			removeIndex = i
			break
		}
	}
	if exist {
		s.topics = append(s.topics[0:removeIndex], s.topics[removeIndex+1:]...)
	}

	if exist {
		s.sm.RemoveSubscriber(s, topic)
	}
}

func (s *Subscribe) isClosed() bool {
	return s.closed
}

func (s *Subscribe) Close() error {
	//todo: 这里不严谨，s.topics 没有锁同步机制，s.topics 不一定是当前最新的,
	//所以目前上层业务层自己保证Close()后没有订阅或退订的操作。一般是连接确定断开后才调用Subscribe.Close(),所以一般没有问题。
	//fmt.Printf("close, %v unSubscribe topics:%v\n", s.id, s.topics)

	//2024-8-24 增加锁
	s.Lock()
	defer s.Unlock()
	if s.isClosed() {
		return fmt.Errorf("subscriber:%v already closed", s.id)
	}
	s.closed = true

	if len(s.topics) == 1 {
		//如果只有一个topic, 直接退订，避免拷贝。大多数也是这种情况
		s.unSubscribe(s.topics[0])
	} else {
		//fixbug:
		//需要把s.topics 拷贝出来，不能直接要用for _, topic := range s.topics {...},
		//因为s.UnSubscribe(topic) 会修改s.topic 底层的数据
		topics := make([]string, len(s.topics))
		copy(topics, s.topics)
		for _, topic := range topics {
			s.unSubscribe(topic)
		}
	}

	//check
	if len(s.topics) != 0 {
		panic(fmt.Sprintf("subscriber:%v closed, but still have topics", s.id))
	}
	return nil
}
