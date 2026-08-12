package strategies

type ChainNode interface {
	Consume(strategy Strategy, data any)
	Then(node ChainNode) ChainNode
}

type StrategyChain interface {
	Strategy
}

type StrategyChainWithNode struct {
	head        ChainNode
	channel     chan any
	stopChannel chan any
}

func (s *StrategyChainWithNode) Receive(data any) {
	s.channel <- data
}

func (s *StrategyChainWithNode) Consume() chan any {
	return s.channel
}

func (s *StrategyChainWithNode) Stop() {
	s.stopChannel <- true
}

func (s *StrategyChainWithNode) Execute(handler func(data any), data any) {
	// handler(data)
}

func (s *StrategyChainWithNode) Start() {
	for {
		select {
		case data := <-s.channel:
			s.head.Consume(s, data)
		case <-s.stopChannel:
			defer close(s.channel)
			defer close(s.stopChannel)
			return
		}
	}
}

type Nextable struct {
	next ChainNode
}

func (n *Nextable) Then(node ChainNode) ChainNode {
	n.next = node
	return node
}
func (n *Nextable) Next(strategy Strategy, data any) {
	if n.next != nil {
		n.next.Consume(strategy, data)
	}
}

type ByPassNode struct {
	Nextable
}

func (b *ByPassNode) Consume(strategy Strategy, data any) {
	b.Next(strategy, data)
}

type ConsumeNode struct {
	channel chan any
	Nextable
}

func (c *ConsumeNode) Consume(strategy Strategy, data any) {
	select {
	case c.channel <- data:
		return
	default:
		c.Next(strategy, data)
	}
}

type DropNodeType = int

const (
	DropLatest DropNodeType = iota
	DropFirst
)

type DropNode struct {
	dropType DropNodeType
}

func (d *DropNode) Consume(strategy Strategy, data any) {
	if d.dropType == DropLatest {
		// Do nothing
	} else {
		<-strategy.Consume()
		strategy.Consume() <- data
	}
}

func NewStartNode() ChainNode {
	return &ByPassNode{}
}
func NewConsumeNode(consume chan any) ChainNode {
	return &ConsumeNode{
		channel: consume,
	}
}
func NewConsumeNodeWithWorkerPool(pool WorkerPool) ChainNode {
	return &ConsumeNode{
		channel: pool.Receive(),
	}
}
func NewStrategyChain(chain ChainNode) StrategyChain {
	strategy := &StrategyChainWithNode{
		head: chain,
	}
	go strategy.Start()

	return strategy
}
