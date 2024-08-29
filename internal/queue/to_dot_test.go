package queue

import (
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestEmptyNetwork(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "digraph {}", NewQueueNetwork().ToDOT())
}

func TestSortedNodeNames(t *testing.T) {
	t.Parallel()

	network := NewQueueNetwork()
	for _, node := range []string{"node3", "node2", "node1"} {
		network.AddNode(node)
	}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{node1|mu = 0.00 req/s}"];
    1 [shape=record,label="{node2|mu = 0.00 req/s}"];
    2 [shape=record,label="{node3|mu = 0.00 req/s}"];
}
`
	assert.Equal(t, strings.TrimSpace(expected), network.ToDOT())
}

func TestLargeNetwork(t *testing.T) {
	t.Parallel()

	network := QueueNetwork{
		NodeMetrics: map[string]*QueueMetric{
			"inventory-db": {durationSum: 52889178, requestCount: 29},
			"cart":         {durationSum: 38462343, requestCount: 10},
			"checkout":     {durationSum: 1058472501, requestCount: 6},
			"payment":      {durationSum: 36145940, requestCount: 2},
			"shipping":     {durationSum: 12603945, requestCount: 2},
			"inventory":    {durationSum: 238685313, requestCount: 16},
			"products-api": {durationSum: 8248710225, requestCount: 37},
			"account":      {durationSum: 46039811, requestCount: 13},
			"account-db":   {durationSum: 41321908, requestCount: 16},
			"cart-redis":   {durationSum: 5722449, requestCount: 11},
			"notification": {durationSum: 1884029, requestCount: 5}},
		incomingRates: map[string]*RateEstimator{
			"products-api": {Estimate: 3.0, totalRequests: 15},
		},
		network: map[string]map[string]uint{
			"account":      {"payment": 1, "products-api": 11, "shipping": 1},
			"account-db":   {"account": 16},
			"cart":         {"checkout": 2, "products-api": 4},
			"cart-redis":   {"cart": 10},
			"checkout":     {"products-api": 1},
			"inventory":    {"cart": 4, "checkout": 1, "products-api": 6},
			"inventory-db": {"inventory": 25},
			"notification": {"inventory": 5},
			"payment":      {"checkout": 1},
			"products-api": {},
			"shipping":     {"checkout": 1},
		},
	}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{account|mu = 282.36 req/s}"];
    1 [shape=record,label="{account-db|mu = 387.20 req/s}"];
    2 [shape=record,label="{cart|mu = 259.99 req/s}"];
    3 [shape=record,label="{cart-redis|mu = 1922.25 req/s}"];
    4 [shape=record,label="{checkout|mu = 5.67 req/s}"];
    5 [shape=record,label="{inventory|mu = 67.03 req/s}"];
    6 [shape=record,label="{inventory-db|mu = 548.32 req/s}"];
    7 [shape=record,label="{notification|mu = 2653.89 req/s}"];
    8 [shape=record,label="{payment|mu = 55.33 req/s}"];
    9 [shape=record,label="{products-api|mu = 4.49 req/s}"];
    10 [shape=record,label="{shipping|mu = 158.68 req/s}"];
    ingress -> 9 [label="3.00 req/s"];
    0 -> 1 [label="1.23"];
    2 -> 3 [label="1.67"];
    2 -> 5 [label="0.67"];
    4 -> 2 [label="2.00"];
    4 -> 5 [label="1.00"];
    4 -> 8 [label="1.00"];
    4 -> 10 [label="1.00"];
    5 -> 6 [label="2.27"];
    5 -> 7 [label="0.45"];
    8 -> 0 [label="1.00"];
    9 -> 0 [label="0.73"];
    9 -> 2 [label="0.27"];
    9 -> 4 [label="0.07"];
    9 -> 5 [label="0.40"];
    10 -> 0 [label="1.00"];
}
`
	assert.Equal(t, strings.TrimSpace(expected), network.ToDOT())
}

func TestMultipleIngressServices(t *testing.T) {
	t.Parallel()

	network := QueueNetwork{
		NodeMetrics: map[string]*QueueMetric{
			"inventory-db": {durationSum: 52889178, requestCount: 29},
			"cart":         {durationSum: 38462343, requestCount: 10},
			"checkout":     {durationSum: 1058472501, requestCount: 6},
			"payment":      {durationSum: 36145940, requestCount: 2},
			"shipping":     {durationSum: 12603945, requestCount: 2},
			"inventory":    {durationSum: 438685313, requestCount: 26},
			"products-api": {durationSum: 8248710225, requestCount: 37},
			"account":      {durationSum: 46739811, requestCount: 18},
			"account-db":   {durationSum: 41321908, requestCount: 16},
			"cart-redis":   {durationSum: 5722449, requestCount: 11},
			"notification": {durationSum: 1884029, requestCount: 5}},
		incomingRates: map[string]*RateEstimator{
			"products-api": {Estimate: 3.0, totalRequests: 15},
			"account":      {Estimate: 2.0, totalRequests: 10},
			"inventory":    {Estimate: 1.0, totalRequests: 5},
		},
		network: map[string]map[string]uint{
			"account":      {"payment": 1, "products-api": 11, "shipping": 1},
			"account-db":   {"account": 16},
			"cart":         {"checkout": 2, "products-api": 4},
			"cart-redis":   {"cart": 10},
			"checkout":     {"products-api": 1},
			"inventory":    {"cart": 4, "checkout": 1, "products-api": 6},
			"inventory-db": {"inventory": 25},
			"notification": {"inventory": 5},
			"payment":      {"checkout": 1},
			"products-api": {},
			"shipping":     {"checkout": 1},
		},
	}

	expected := `
digraph {
    ingress [label="ingress"];
    0 [shape=record,label="{account|mu = 385.11 req/s}"];
    1 [shape=record,label="{account-db|mu = 387.20 req/s}"];
    2 [shape=record,label="{cart|mu = 259.99 req/s}"];
    3 [shape=record,label="{cart-redis|mu = 1922.25 req/s}"];
    4 [shape=record,label="{checkout|mu = 5.67 req/s}"];
    5 [shape=record,label="{inventory|mu = 59.27 req/s}"];
    6 [shape=record,label="{inventory-db|mu = 548.32 req/s}"];
    7 [shape=record,label="{notification|mu = 2653.89 req/s}"];
    8 [shape=record,label="{payment|mu = 55.33 req/s}"];
    9 [shape=record,label="{products-api|mu = 4.49 req/s}"];
    10 [shape=record,label="{shipping|mu = 158.68 req/s}"];
    ingress -> 0 [label="2.00 req/s"];
    ingress -> 5 [label="1.00 req/s"];
    ingress -> 9 [label="3.00 req/s"];
    0 -> 1 [label="0.70"];
    2 -> 3 [label="1.67"];
    2 -> 5 [label="0.67"];
    4 -> 2 [label="2.00"];
    4 -> 5 [label="1.00"];
    4 -> 8 [label="1.00"];
    4 -> 10 [label="1.00"];
    5 -> 6 [label="1.56"];
    5 -> 7 [label="0.31"];
    8 -> 0 [label="1.00"];
    9 -> 0 [label="0.73"];
    9 -> 2 [label="0.27"];
    9 -> 4 [label="0.07"];
    9 -> 5 [label="0.40"];
    10 -> 0 [label="1.00"];
}
`
	assert.Equal(t, strings.TrimSpace(expected), network.ToDOT())
}
