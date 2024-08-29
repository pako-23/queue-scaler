package queue

import "time"

func (q *QueueNetwork) incomingRequests() map[string]uint {
	incomingRequests := make(map[string]uint, len(q.network)+len(q.incomingRates))
	for node, value := range q.incomingRates {
		incomingRequests[node] += value.totalRequests
	}

	for node, incoming := range q.network {
		for _, weight := range incoming {
			if _, ok := incomingRequests[node]; !ok {
				incomingRequests[node] = 0
			}

			incomingRequests[node] += weight
		}
	}

	return incomingRequests
}

func (q *QueueNetwork) incomingRate(node string, requests map[string]uint, rates map[string]float64) float64 {
	rate := 0.0

	if estimator, ok := q.incomingRates[node]; ok {
		rate += estimator.Estimate
	}

	for from, weight := range q.network[node] {
		prob := float64(weight) / float64(requests[from])

		if _, ok := rates[from]; !ok {
			rates[from] = q.incomingRate(from, requests, rates)
		}

		rate += prob * rates[from]
	}

	rates[node] = rate

	return rate
}

func (q *QueueNetwork) UpdateEstimates(interval time.Duration) {
	for _, estimator := range q.incomingRates {
		estimator.Update(interval)
	}
}

func (q *QueueNetwork) IncomingRates() map[string]float64 {
	incomingRequests := q.incomingRequests()
	incomingRates := make(map[string]float64, len(q.network))

	for node := range q.network {
		q.incomingRate(node, incomingRequests, incomingRates)
	}

	return incomingRates
}
