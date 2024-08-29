package queue

import "github.com/pako-23/queue-scaler/internal/receiver"

type QueueNetwork struct {
	NodeMetrics   map[string]*QueueMetric
	incomingRates map[string]*RateEstimator
	network       map[string]map[string]uint
}

func NewQueueNetwork() *QueueNetwork {
	return &QueueNetwork{
		NodeMetrics:   map[string]*QueueMetric{},
		incomingRates: map[string]*RateEstimator{},
		network:       map[string]map[string]uint{},
	}
}

func (q *QueueNetwork) AddNode(node string) {
	if _, ok := q.network[node]; !ok {
		q.network[node] = map[string]uint{}
		q.NodeMetrics[node] = &QueueMetric{
			durationSum:  0,
			requestCount: 0,
		}
	}
}

func (q *QueueNetwork) AddExternalRequest(request *receiver.Span) {
	q.AddNode(request.ServiceName)
	q.NodeMetrics[request.ServiceName].durationSum += request.Duration
	q.NodeMetrics[request.ServiceName].requestCount += 1

	if _, ok := q.incomingRates[request.ServiceName]; !ok {
		q.incomingRates[request.ServiceName] = &RateEstimator{
			Estimate:       0.0,
			latestRequests: 0,
			totalRequests:  0,
		}
	}
	q.incomingRates[request.ServiceName].latestRequests += 1
	q.incomingRates[request.ServiceName].totalRequests += 1
}

func (q *QueueNetwork) AddInternalRequest(parent *receiver.Span, request *receiver.Span) {
	q.AddNode(request.ServiceName)
	q.NodeMetrics[request.ServiceName].durationSum += request.Duration
	q.NodeMetrics[request.ServiceName].requestCount += 1

	if parent.ServiceName == request.ServiceName {
		return
	}

	q.AddNode(parent.ServiceName)

	if count, ok := q.network[request.ServiceName][parent.ServiceName]; ok {
		q.network[request.ServiceName][parent.ServiceName] = count + 1
	} else {
		q.network[request.ServiceName][parent.ServiceName] = 1
	}
}
