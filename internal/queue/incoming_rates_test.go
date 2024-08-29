package queue

import (
	"fmt"
	"math"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func compareIncomingRates(expected map[string]float64, value map[string]float64) cmp.Comparison {
	return func() cmp.Result {
		if len(expected) != len(value) {
			return cmp.ResultFailure(
				fmt.Sprintf(
					"the two incoming rates have different sizes. expected %d, got %d",
					len(expected), len(value)))
		}

		for service, rate := range expected {
			if gotRate, ok := value[service]; !ok {
				return cmp.ResultFailure(
					fmt.Sprintf("expected service '%s' in replicas, but not found",
						service))
			} else if math.Abs(gotRate-rate) > 10e-9 {
				return cmp.ResultFailure(
					fmt.Sprintf("expected service '%s' to have %f req/s, got %f req/s",
						service, rate, gotRate))
			}
		}

		return cmp.ResultSuccess
	}
}

func compareEstimates(expected map[string]float64, value *QueueNetwork) cmp.Comparison {
	return func() cmp.Result {
		if len(expected) != len(value.incomingRates) {
			return cmp.ResultFailure(
				fmt.Sprintf(
					"the sets of estimates ahve different sizes. expected %d, got %d",
					len(expected), len(value.incomingRates)))

		}

		for service, estimate := range expected {
			if estimator, ok := value.incomingRates[service]; !ok {
				return cmp.ResultFailure(
					fmt.Sprintf("missing the incoming rate estimate for serivice '%s'",
						service))
			} else if math.Abs(estimate-estimator.Estimate) >= 10e-9 {
				return cmp.ResultFailure(
					fmt.Sprintf("estimate for service '%s' is %f, expected %f",
						service, estimator.Estimate, estimate))
			}
		}

		return cmp.ResultSuccess
	}
}

func TestRatesEmptyNetwork(t *testing.T) {
	t.Parallel()

	network := NewQueueNetwork()
	assert.Assert(t, compareIncomingRates(map[string]float64{}, network.IncomingRates()))
}

func TestRatesOnlyNodes(t *testing.T) {
	t.Parallel()

	network := NewQueueNetwork()

	for _, node := range []string{"node1", "node2", "node3"} {
		network.AddNode(node)
	}

	expected := map[string]float64{
		"node1": 0.0,
		"node2": 0.0,
		"node3": 0.0,
	}

	assert.Assert(t, compareIncomingRates(expected, network.IncomingRates()))
}

func TestSinglePath(t *testing.T) {
	t.Parallel()

	network := QueueNetwork{
		NodeMetrics: map[string]*QueueMetric{
			"node1": {durationSum: 2600000000, requestCount: 100},
			"node2": {durationSum: 100000000, requestCount: 100},
			"node3": {durationSum: 1000000000, requestCount: 100},
			"node4": {durationSum: 100000000, requestCount: 100},
		},
		network: map[string]map[string]uint{
			"node1": {},
			"node2": {"node1": 100},
			"node3": {"node2": 100},
			"node4": {"node3": 100},
		},
		incomingRates: map[string]*RateEstimator{
			"node1": {
				Estimate:      100.0,
				totalRequests: 100,
			},
		},
	}

	expected := map[string]float64{
		"node1": 100.0,
		"node2": 100.0,
		"node3": 100.0,
		"node4": 100.0,
	}

	assert.Assert(t, compareIncomingRates(expected, network.IncomingRates()))
}

func TestSplitTraffic(t *testing.T) {
	t.Parallel()

	network := QueueNetwork{
		NodeMetrics: map[string]*QueueMetric{
			"node1": {durationSum: 100000000, requestCount: 100},
			"node2": {durationSum: 3000000000, requestCount: 30},
			"node3": {durationSum: 1000000000, requestCount: 70},
			"node4": {durationSum: 2600000000, requestCount: 100},
		},
		incomingRates: map[string]*RateEstimator{
			"node1": {
				Estimate:      100.0,
				totalRequests: 100,
			},
		},
		network: map[string]map[string]uint{
			"node1": {},
			"node2": {"node1": 30},
			"node3": {"node1": 70},
			"node4": {"node2": 30, "node3": 70},
		},
	}

	expected := map[string]float64{
		"node1": 100.0,
		"node2": 30.0,
		"node3": 70.0,
		"node4": 100.0,
	}

	assert.Assert(t, compareIncomingRates(expected, network.IncomingRates()))
}

func TestRatesLargeNetwork(t *testing.T) {
	t.Parallel()

	network := QueueNetwork{
		incomingRates: map[string]*RateEstimator{
			"products-api": {
				Estimate:      100.0,
				totalRequests: 100,
			},
		},
		NodeMetrics: map[string]*QueueMetric{
			"inventory-db": {durationSum: 500000000, requestCount: 140},
			"cart":         {durationSum: 50000000, requestCount: 6},
			"checkout":     {durationSum: 50000000, requestCount: 1},
			"payment":      {durationSum: 10000000, requestCount: 1},
			"shipping":     {durationSum: 10000000, requestCount: 1},
			"inventory":    {durationSum: 2500000000, requestCount: 88},
			"products-api": {durationSum: 2500000000, requestCount: 100},
			"account":      {durationSum: 1300000000, requestCount: 19},
			"account-db":   {durationSum: 500000000, requestCount: 30},
			"cart-redis":   {durationSum: 2500000, requestCount: 5},
			"notification": {durationSum: 25000000, requestCount: 60},
		},
		network: map[string]map[string]uint{
			"account":      {"payment": 1, "products-api": 17, "shipping": 1},
			"account-db":   {"account": 30},
			"cart":         {"checkout": 2, "products-api": 4},
			"cart-redis":   {"cart": 5},
			"checkout":     {"products-api": 1},
			"inventory":    {"cart": 1, "checkout": 1, "products-api": 86},
			"inventory-db": {"inventory": 140},
			"notification": {"inventory": 60},
			"payment":      {"checkout": 1},
			"products-api": {},
			"shipping":     {"checkout": 1},
		},
	}

	expected := map[string]float64{
		"inventory-db": 140.0,
		"cart":         6.0,
		"checkout":     1.0,
		"payment":      1.0,
		"shipping":     1.0,
		"inventory":    88.0,
		"products-api": 100.0,
		"account":      19.0,
		"account-db":   30.0,
		"cart-redis":   5.0,
		"notification": 60.0,
	}

	assert.Assert(t, compareIncomingRates(expected, network.IncomingRates()))
}

func TestRatesUpdate(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		network  *QueueNetwork
		expected map[string]float64
	}{
		{
			network:  NewQueueNetwork(),
			expected: map[string]float64{},
		},
		{
			network: &QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {durationSum: 100000000, requestCount: 100},
				},
				incomingRates: map[string]*RateEstimator{
					"node1": {
						Estimate:       0.0,
						latestRequests: 100.0,
					},
				},
				network: map[string]map[string]uint{
					"node1": {},
				},
			},
			expected: map[string]float64{"node1": 80.0},
		},
		{
			network: &QueueNetwork{
				NodeMetrics: map[string]*QueueMetric{
					"node1": {durationSum: 100000000, requestCount: 100},
					"node2": {durationSum: 100000000, requestCount: 100},
				},
				incomingRates: map[string]*RateEstimator{
					"node1": {
						Estimate:       0.0,
						latestRequests: 100.0,
					},
					"node2": {
						Estimate:       40.0,
						latestRequests: 40,
					},
				},
				network: map[string]map[string]uint{
					"node1": {},
					"node2": {},
				},
			},
			expected: map[string]float64{"node1": 80.0, "node2": 40.0},
		},
	}

	for _, test := range tests {
		test.network.UpdateEstimates(time.Second)
		assert.Assert(t, compareEstimates(test.expected, test.network))
	}
}
