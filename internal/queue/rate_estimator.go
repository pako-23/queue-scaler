package queue

import "time"

const alpha = 0.8

type RateEstimator struct {
	Estimate       float64
	latestRequests uint
	totalRequests  uint
}

func (r *RateEstimator) Update(interval time.Duration) {
	r.Estimate = (1-alpha)*r.Estimate + alpha*(float64(r.latestRequests)/interval.Seconds())
	r.latestRequests = 0
}
