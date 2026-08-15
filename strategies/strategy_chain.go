package strategies

import "log"

type ChainNode interface {
	Consume(strategy Strategy, data any)
	Then(node ChainNode) ChainNode
	Stop(strategy Strategy)
}

type StrategyChain interface {
	Strategy
}

type StrategyChainWithNode struct {
	head ChainNode
}

func (s *StrategyChainWithNode) Receive(data any) {
	log.Printf("chain strategy received %s", data)
	s.head.Consume(s, data)
}

func (s *StrategyChainWithNode) Consume() chan any {
	return nil
}

func (s *StrategyChainWithNode) Stop() {
	s.head.Stop(s)
}

func (s *StrategyChainWithNode) Execute(handler func(data any), data any) {
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
		log.Printf("Executing next node")
		n.next.Consume(strategy, data)
	} else {
		log.Printf("This is the last node, end")
	}
}
func (n *Nextable) Stop(strategy Strategy) {
	if n.next != nil {
		n.next.Stop(strategy)
	}
}

type ByPassNode struct {
	Nextable
}

func (b *ByPassNode) Consume(strategy Strategy, data any) {
	log.Printf("By passing node...")
	b.Next(strategy, data)
}

type ConsumeNode struct {
	pool WorkerPool
	Nextable
}

func (c *ConsumeNode) Stop(strategy Strategy) {
	c.pool.Stop()
	c.Nextable.Stop(strategy)
}
func (c *ConsumeNode) Consume(strategy Strategy, data any) {
	log.Printf("consume node receive %v", data)
	select {
	case c.pool.Receive() <- data:
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
}

func NewStartNode() ChainNode {
	return &ByPassNode{}
}
func NewConsumeNode(pool WorkerPool) ChainNode {
	go pool.Start()
	return &ConsumeNode{
		pool: pool,
	}
}
func NewStrategyChain(chain ChainNode) StrategyChain {
	strategy := &StrategyChainWithNode{
		head: chain,
	}

	return strategy
}

func FromSlice(nodes []ChainNode) ChainNode {
	head := nodes[0]
	current := head
	for i := 1; i < len(nodes); i++ {
		current.Then(nodes[i])
		current = nodes[i]
	}
	return head
}
