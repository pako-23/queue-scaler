package queue

import (
	"fmt"
	"math"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func compareFloats(value float64, expected float64, eps float64) cmp.Comparison {
	return func() cmp.Result {
		if math.Abs(expected-value) > eps {
			return cmp.ResultFailure(fmt.Sprintf("expected %f, but got %f", expected, value))
		}

		return cmp.ResultSuccess
	}
}

func TestEstimate(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		requests uint
		interval time.Duration
		expected float64
	}{
		{
			requests: 10,
			interval: time.Second,
			expected: 8.0,
		},
		{
			requests: 10,
			interval: 5 * time.Second,
			expected: 1.6,
		},
		{
			requests: 0,
			interval: time.Second,
			expected: 0,
		},
	}

	for _, test := range tests {
		estimator := &RateEstimator{
			Estimate:       0.0,
			latestRequests: test.requests,
			totalRequests:  0}
		estimator.Update(test.interval)
		assert.Equal(t, uint(0), estimator.latestRequests,
			"After updating the estimate, the total number of requests should be 0")
		assert.Assert(t, compareFloats(estimator.Estimate, test.expected, 10e-9))
	}
}
