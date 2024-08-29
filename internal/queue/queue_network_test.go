package queue

import (
	"fmt"
	"testing"

	"github.com/pako-23/queue-scaler/internal/receiver"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func queueNetworkComparer(value *QueueNetwork, expected *QueueNetwork) cmp.Comparison {
	return func() cmp.Result {
		// compare incoming rates
		if len(value.incomingRates) != len(expected.incomingRates) {
			return cmp.ResultFailure(
				fmt.Sprintf("the network has incoming rates from %d nodes, expected %d",
					len(value.incomingRates), len(expected.incomingRates)))
		}

		for service, rate := range expected.incomingRates {
			if gotRate, ok := value.incomingRates[service]; !ok {
				return cmp.ResultFailure(
					fmt.Sprintf("expected service '%s' in incoming rates, but not found",
						service))
			} else if *gotRate != *rate {
				return cmp.ResultFailure(
					fmt.Sprintf("expected incoming rate '%v', but got '%v'",
						*rate, *gotRate))
			}
		}

		// compare metrics
		if len(value.NodeMetrics) != len(expected.NodeMetrics) {
			return cmp.ResultFailure(
				fmt.Sprintf("the network has metrics for %d nodes, expected %d",
					len(value.NodeMetrics), len(expected.NodeMetrics)))
		}

		for service, metric := range expected.NodeMetrics {
			if gotMetric, ok := value.NodeMetrics[service]; !ok {
				return cmp.ResultFailure(
					fmt.Sprintf("expected service '%s' in node metrics, but not found",
						service))
			} else if *gotMetric != *metric {
				return cmp.ResultFailure(
					fmt.Sprintf("expected node metric '%v', but got '%v'",
						*metric, *gotMetric))
			}
		}

		// compare network
		if len(value.network) != len(expected.network) {
			return cmp.ResultFailure(
				fmt.Sprintf("the network has %d nodes, expected %d",
					len(value.network), len(expected.network)))
		}

		for u, edges := range expected.network {
			gotEdges, ok := value.network[u]

			if !ok {
				return cmp.ResultFailure(
					fmt.Sprintf("expected node '%s' in the queue network, but not found", u))
			}

			for v, weight := range edges {
				if gotWeight, ok := gotEdges[v]; !ok {
					return cmp.ResultFailure(
						fmt.Sprintf("expected edge from '%s' to '%s', but not found",
							u, v))
				} else if gotWeight != weight {
					return cmp.ResultFailure(
						fmt.Sprintf(
							"edge from '%s' to '%s' has weight %d, but expected %d",
							u, v, gotWeight, weight))
				}
			}
		}

		return cmp.ResultSuccess
	}
}

func TestQueueNetworkInit(t *testing.T) {
	t.Parallel()

	value := NewQueueNetwork()
	assert.Assert(t, value != nil)
	assert.Assert(t, queueNetworkComparer(value, &QueueNetwork{
		NodeMetrics:   map[string]*QueueMetric{},
		incomingRates: map[string]*RateEstimator{},
		network:       map[string]map[string]uint{},
	}))

}

func TestAddNode(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		nodes    []string
		expected QueueNetwork
	}{
		{
			nodes:    []string{},
			expected: QueueNetwork{},
		},
		{
			nodes: []string{"node1"},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{"node1": {}},
				network:     map[string]map[string]uint{"node1": {}},
			},
		},
		{
			nodes: []string{"node1", "node2", "node3"},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {},
					"node2": {},
					"node3": {}},
				network: map[string]map[string]uint{
					"node1": {},
					"node2": {},
					"node3": {},
				},
			},
		},
		{
			nodes: []string{"node1", "node1", "node1"},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{"node1": {}},
				network:     map[string]map[string]uint{"node1": {}},
			},
		},
		{
			nodes: []string{"node5", "node1", "node4", "node3", "node1", "node2", "node4"},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {},
					"node2": {},
					"node3": {},
					"node4": {},
					"node5": {},
				},
				network: map[string]map[string]uint{
					"node1": {},
					"node2": {},
					"node3": {},
					"node4": {},
					"node5": {},
				}},
		},
	}

	for _, test := range tests {
		value := NewQueueNetwork()
		assert.Check(t, value != nil)

		for _, node := range test.nodes {
			value.AddNode(node)
		}

		assert.Assert(t, queueNetworkComparer(value, &test.expected))
	}
}

func TestAddExternalRequest(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		requests []*receiver.Span
		expected QueueNetwork
	}{
		{
			requests: []*receiver.Span{},
			expected: QueueNetwork{},
		},
		{
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1000,
						requestCount: 1,
					},
				},
				network: map[string]map[string]uint{"node1": {}},
				incomingRates: map[string]*RateEstimator{
					"node1": {totalRequests: 1, latestRequests: 1},
				},
			},
		},
		{
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node1",
					Duration:    500,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1500,
						requestCount: 2,
					},
				},
				incomingRates: map[string]*RateEstimator{
					"node1": {totalRequests: 2, latestRequests: 2},
				},
				network: map[string]map[string]uint{"node1": {}},
			},
		},
		{
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node2",
					Duration:    500,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1000,
						requestCount: 1,
					},
					"node2": {
						durationSum:  500,
						requestCount: 1,
					},
				},

				incomingRates: map[string]*RateEstimator{
					"node1": {totalRequests: 1, latestRequests: 1},
					"node2": {totalRequests: 1, latestRequests: 1},
				},
				network: map[string]map[string]uint{"node1": {}, "node2": {}},
			},
		},
		{
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node2",
					Duration:    500,
				},
				{
					ServiceName: "node1",
					Duration:    1000,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  2000,
						requestCount: 2,
					},
					"node2": {
						durationSum:  500,
						requestCount: 1,
					},
				},
				incomingRates: map[string]*RateEstimator{
					"node1": {totalRequests: 2, latestRequests: 2},
					"node2": {totalRequests: 1, latestRequests: 1},
				},
				network: map[string]map[string]uint{"node1": {}, "node2": {}},
			},
		},
	}

	for _, test := range tests {
		value := NewQueueNetwork()
		assert.Assert(t, value != nil)

		for _, span := range test.requests {
			value.AddExternalRequest(span)
		}

		assert.Assert(t, queueNetworkComparer(value, &test.expected))
	}
}

func TestAddInternalRequest(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		parents  []string
		requests []*receiver.Span
		expected QueueNetwork
	}{
		{
			parents:  []string{},
			requests: []*receiver.Span{},
			expected: QueueNetwork{network: map[string]map[string]uint{}},
		},
		{
			parents: []string{"node2"},
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1000,
						requestCount: 1,
					},
					"node2": {},
				},
				network: map[string]map[string]uint{"node1": {"node2": 1}, "node2": {}},
			},
		},
		{
			parents: []string{"node2", "node2"},
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node1",
					Duration:    500,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1500,
						requestCount: 2,
					},
					"node2": {},
				},
				network: map[string]map[string]uint{"node1": {"node2": 2}, "node2": {}},
			},
		},
		{
			parents: []string{"node2", "node2"},
			requests: []*receiver.Span{
				{
					ServiceName: "node2",
					Duration:    1000,
				},
				{
					ServiceName: "node2",
					Duration:    500,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node2": {
						durationSum:  1500,
						requestCount: 2,
					},
				},
				network: map[string]map[string]uint{"node2": {}},
			},
		},
		{
			parents: []string{"node2", "node2", "node2", "node2", "node1"},
			requests: []*receiver.Span{
				{
					ServiceName: "node2",
					Duration:    1000,
				},
				{
					ServiceName: "node2",
					Duration:    500,
				},
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node1",
					Duration:    500,
				},
				{
					ServiceName: "node3",
					Duration:    200,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  1500,
						requestCount: 2,
					},
					"node2": {
						durationSum:  1500,
						requestCount: 2,
					},
					"node3": {
						durationSum:  200,
						requestCount: 1,
					},
				},
				network: map[string]map[string]uint{
					"node1": {"node2": 2},
					"node2": {},
					"node3": {"node1": 1},
				},
			},
		},
		{
			parents: []string{"node2", "node2", "node2", "node3", "node3", "node3", "node3"},
			requests: []*receiver.Span{
				{
					ServiceName: "node1",
					Duration:    1000,
				},
				{
					ServiceName: "node1",
					Duration:    500,
				},
				{
					ServiceName: "node1",
					Duration:    500,
				},
				{
					ServiceName: "node1",
					Duration:    200,
				},
				{
					ServiceName: "node1",
					Duration:    200,
				},
				{
					ServiceName: "node1",
					Duration:    200,
				},
				{
					ServiceName: "node1",
					Duration:    200,
				},
			},
			expected: QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {
						durationSum:  2800,
						requestCount: 7,
					},
					"node2": {},
					"node3": {},
				},
				network: map[string]map[string]uint{
					"node1": {"node2": 3, "node3": 4},
					"node2": {},
					"node3": {},
				},
			},
		},
	}

	for _, test := range tests {
		value := NewQueueNetwork()
		assert.Assert(t, value != nil)
		assert.Assert(t, len(test.parents) == len(test.requests))

		for i := range test.requests {
			value.AddInternalRequest(
				&receiver.Span{ServiceName: test.parents[i]}, test.requests[i])
		}

		assert.Assert(t, queueNetworkComparer(value, &test.expected))
	}
}
