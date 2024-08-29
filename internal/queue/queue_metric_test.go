package queue

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestServiceRateNoRequests(t *testing.T) {
	t.Parallel()
	metric := &QueueMetric{}
	assert.Assert(t, compareFloats(metric.ServiceRate(), 0.0, 10e-9))
}

func TestServiceRate(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		metric   *QueueMetric
		expected float64
	}{
		{
			metric: &QueueMetric{
				durationSum:  1000000000,
				requestCount: 1,
			},
			expected: 1.0,
		},
		{
			metric: &QueueMetric{
				durationSum:  1000000000,
				requestCount: 10,
			},
			expected: 10.0,
		},
		{
			metric: &QueueMetric{
				durationSum:  1000000000,
				requestCount: 100,
			},
			expected: 100.0,
		},
	}

	for _, test := range tests {
		assert.Assert(t, compareFloats(test.metric.ServiceRate(), test.expected, 10e-9))
	}
}
