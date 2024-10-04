package subscribe

import (
	"fmt"
	"sync"
)

const (
	TopicAdd = "add"
	TopicDel = "del"
)

type TopicWatcher struct {
	name  string
	event chan WatchTopicEvent
}

type WatchTopicEvent struct {
	Op    string
	Topic string
}

func (watcher *TopicWatcher) Event() <-chan WatchTopicEvent {
	return watcher.event
}

func (watcher *TopicWatcher) Name() string {
	return watcher.name
}

type SubscriberMgr struct {
	sync.RWMutex
	subscribers     map[string]*Subscribers //key: topic,
	subscriberCheck func(from Subscriber, topic string) error
	publishCheck    func(from Subscriber, topic string, data []byte) error

	watchMu       sync.RWMutex
	topicWatchers map[string]*TopicWatcher //key: watchId, value: struct{}
}

type subscriberCheckHandler func(from Subscriber, topic string) error
type publishCheckHandler func(from Subscriber, topic string, data []byte) error
type SubscriberMgrOpt func(*SubscriberMgr)

type Subscribers struct {
	sync.RWMutex
	subMap   map[SubscriberID]Subscriber
	subSlice []Subscriber
}

var defaultSubscriberMgr *SubscriberMgr

func init() {
	defaultSubscriberMgr = NewSubscriberMgr()
}

func NewSubscriber(id SubscriberID, h MailBoxHandler) Subscriber {
	return defaultSubscriberMgr.NewSubscriber(id, h)
}

func (ss *Subscribers) GetSubscriber(id SubscriberID) Subscriber {
	ss.RLock()
	defer ss.RUnlock()
	return ss.subMap[id]
}

// 在锁保护下, 遍历所有的subscriber, 不包括exclude的subscriber
// todo: 通过sharding 把锁的颗粒度改小, 尽量减少锁的竞争
func (ss *Subscribers) TraversalDo(exclude Subscriber, do func(s Subscriber)) {
	ss.RLock()
	defer ss.RUnlock()
	for _, s := range ss.subMap {
		if s == exclude {
			continue
		}
		do(s)
	}
}

func (ss *Subscribers) GetAllSubscriber() []Subscriber {
	ss.RLock()
	defer ss.RUnlock()
	return ss.subSlice
}

func (ss *Subscribers) DelSubscriber(s Subscriber) {
	//fmt.Printf("delelte subscriber:%v\n", s.Id())
	if s == nil {
		return
	}
	ss.Lock()
	defer ss.Unlock()

	delete(ss.subMap, s.Id())

	//下面就是从slice 中删除指定的subscriber, 删除前判断下slice 占用的内存和实际保存的数据是否相差太大
	//如果slice 的长度小于 slice cap的1/3(一次append可能就会使cap翻倍了)，那么为了节约内存，应该重新生成subSlice
	if len(ss.subSlice) > 128 && len(ss.subSlice) < cap(ss.subSlice)/3 {
		ss.subSlice = make([]Subscriber, 0, len(ss.subMap))
		for _, s := range ss.subMap {
			ss.subSlice = append(ss.subSlice, s)
		}
		return
	}
	//从sub slice 中删除指定的subscriber
	removeIndex := 0
	exist := false
	for i, v := range ss.subSlice {
		if v == s {
			exist = true
			removeIndex = i
			break
		}
	}
	if exist {
		ss.subSlice = append(ss.subSlice[0:removeIndex], ss.subSlice[removeIndex+1:]...)
	}

	//check
	if len(ss.subMap) != len(ss.subSlice) {
		panic(fmt.Sprintf("len(ss.subMap):%d != len(ss.subSlice):%d", len(ss.subMap), len(ss.subSlice)))
	}
}

func (ss *Subscribers) AddSubscriber(s Subscriber) {
	ss.Lock()
	defer ss.Unlock()
	ss.subMap[s.Id()] = s
	ss.subSlice = append(ss.subSlice, s) //翻倍分配内存

	//check
	if len(ss.subMap) != len(ss.subSlice) {
		panic(fmt.Sprintf("len(ss.subMap):%d != len(ss.subSlice):%d", len(ss.subMap), len(ss.subSlice)))
	}
}

// 有订阅者订阅主题时，可以检测这个订阅者，以判断是否允许这个订阅者订阅。
func WithSubscribeCheck(h subscriberCheckHandler) SubscriberMgrOpt {
	return func(sm *SubscriberMgr) {
		sm.subscriberCheck = h
	}
}

// 用于在发布消息时，上层业务可以根据自己的需求控制哪些消息可以公告发布
func WithPublishCheck(h publishCheckHandler) SubscriberMgrOpt {
	return func(sm *SubscriberMgr) {
		sm.publishCheck = h
	}
}

func NewSubscriberMgr(opts ...SubscriberMgrOpt) *SubscriberMgr {
	sm := &SubscriberMgr{subscribers: make(map[string]*Subscribers), topicWatchers: make(map[string]*TopicWatcher)}
	for _, opt := range opts {
		opt(sm)
	}
	return sm
}

func (sm *SubscriberMgr) NewSubscriber(id SubscriberID, h MailBoxHandler) Subscriber {
	return &Subscribe{id: id, handler: h, sm: sm}
}

func (sm *SubscriberMgr) AddSubscriber(topic string, s Subscriber) error {
	if sm.subscriberCheck != nil {
		if err := sm.subscriberCheck(s, topic); err != nil {
			return err
		}
	}

	sm.Lock()
	defer sm.Unlock()

	subscribers, ok := sm.subscribers[topic]
	if !ok {
		subscribers = &Subscribers{subMap: make(map[SubscriberID]Subscriber)}
		subscribers.AddSubscriber(s)
		sm.subscribers[topic] = subscribers
		sm.NotifyTopicsWatcher(TopicAdd, topic)
		return nil
	}
	subscribers.AddSubscriber(s)
	return nil
}

func (sm *SubscriberMgr) RemoveSubscriberByID(id SubscriberID, topic string) {
	sm.Lock()
	defer sm.Unlock()
	subscribers, ok := sm.subscribers[topic]
	if !ok {
		return
	}

	sub := subscribers.GetSubscriber(id)
	subscribers.DelSubscriber(sub)

	//there is no subscribers on this topic?
	if len(subscribers.subMap) == 0 {
		delete(sm.subscribers, topic)
	}
}

// there is no subscribers on this topic?
func (sm *SubscriberMgr) RemoveSubscriber(sub Subscriber, topic string) {
	//todo: 把锁的颗粒度改小点
	sm.Lock()
	defer sm.Unlock()
	sm.removeSubscriber(sub, topic)
}

func (sm *SubscriberMgr) removeSubscriber(sub Subscriber, topic string) {
	subscribers, ok := sm.subscribers[topic]
	if !ok {
		return
	}

	subscribers.DelSubscriber(sub)

	//there is no subscribers on this topic?
	if len(subscribers.subMap) == 0 {
		delete(sm.subscribers, topic)
		sm.NotifyTopicsWatcher(TopicDel, topic)
	}
}

func (sm *SubscriberMgr) PublishFromSubID(from SubscriberID, topic string, data []byte) (int, error) {
	if from == "" {
		return sm.Publish(nil, topic, data)
	}
	sm.Lock()
	subscribers, ok := sm.subscribers[topic]
	if !ok {
		sm.Unlock()
		return 0, fmt.Errorf("no topic:%s", topic)
	}
	sm.Unlock()

	return sm.Publish(subscribers.GetSubscriber(from), topic, data)
}

// from可以为nil,即所有的订阅者都会收到，from 不为空，除from自己收不到，其他订阅者都能收到
// 返回发送给订阅者的数量
func (sm *SubscriberMgr) Publish(from Subscriber, topic string, data []byte) (int, error) {
	// 订阅者也可以发布消息的话, 这里需要检查from是否有权限发布这个topic的消息，同时也可以检查消息内容是否合规之类的
	if sm.publishCheck != nil {
		if err := sm.publishCheck(from, topic, data); err != nil {
			return 0, err
		}
	}

	sm.Lock()
	subscribers, ok := sm.subscribers[topic]
	if !ok {
		sm.Unlock()
		return 0, fmt.Errorf("no topic:%s", topic)
	}
	sm.Unlock()

	n := 0
	// //topicSubscribers 可能会有并发问题, 可能会被修改，必须加锁，但是加锁影响性能，可以sharding 来减少锁的影响。
	// topicSubscribers := subscribers.getAllSubscriber()
	// for _, s := range topicSubscribers {
	// 	if s == from {
	// 		continue
	// 	}
	// 	n++
	// 	s.MailBoxMsg(topic, data)
	// }

	// 加锁遍历给所有订阅者发送消息, 先保证正确性
	subscribers.TraversalDo(from, func(s Subscriber) {
		n++
		s.MailBoxMsg(topic, data)
	})
	return n, nil
}

func (sm *SubscriberMgr) GetSubscribers(topic string) []Subscriber {
	sm.Lock()
	subscribers, ok := sm.subscribers[topic]
	if !ok {
		sm.Unlock()
		return nil
	}
	sm.Unlock()

	return subscribers.GetAllSubscriber()
}

func (sm *SubscriberMgr) TopicNum() int {
	sm.Lock()
	defer sm.Unlock()
	return len(sm.subscribers)
}

func (sm *SubscriberMgr) Topics() []string {
	sm.Lock()
	defer sm.Unlock()
	topics := make([]string, 0, len(sm.subscribers))
	for topic := range sm.subscribers {
		topics = append(topics, topic)
	}
	return topics
}

func (sm *SubscriberMgr) NewTopicsWatcher(watchId string) (*TopicWatcher, error) {
	sm.watchMu.Lock()
	defer sm.watchMu.Unlock()

	_, ok := sm.topicWatchers[watchId]
	if ok {
		return nil, fmt.Errorf("watchId:%s already exists", watchId)
	}

	watcher := &TopicWatcher{name: watchId, event: make(chan WatchTopicEvent, 128)}
	sm.topicWatchers[watchId] = watcher
	return watcher, nil
}

func (sm *SubscriberMgr) DelTopicsWatcher(watchId string) {
	sm.watchMu.Lock()
	defer sm.watchMu.Unlock()
	if watcher, ok := sm.topicWatchers[watchId]; ok {
		delete(sm.topicWatchers, watchId)
		close(watcher.event)
	}
}

func (sm *SubscriberMgr) NotifyTopicsWatcher(op string, topic string) {
	sm.watchMu.RLock()
	defer sm.watchMu.RUnlock()
	info := WatchTopicEvent{Op: op, Topic: topic}
	for _, watcher := range sm.topicWatchers {
		select {
		case watcher.event <- info:
		default:
		}
	}
}
