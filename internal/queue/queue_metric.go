package queue

type QueueMetric struct {
	durationSum  uint64
	requestCount uint64
}

func (q *QueueMetric) ServiceRate() float64 {
	if q.requestCount == 0 {
		return 0.0
	}

	return 1.0 / ((float64(q.durationSum) / 1e9) / float64(q.requestCount))
}
