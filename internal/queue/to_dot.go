package queue

import (
	"fmt"
	"sort"
	"strings"
)

func (q *QueueNetwork) ToDOT() string {
	var builder strings.Builder

	builder.WriteString("digraph {")
	if len(q.network) == 0 {
		builder.WriteString("}")
		return builder.String()
	}

	nodes := make([]string, 0, len(q.network))
	for node := range q.network {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	builder.WriteString("\n    ingress [label=\"ingress\"];\n")
	index := make(map[string]int, len(nodes))
	for i, node := range nodes {
		builder.WriteString(
			fmt.Sprintf("    %d [shape=record,label=\"{%s|mu = %.2f req/s}\"];\n",
				i, node, q.NodeMetrics[node].ServiceRate()))
		index[node] = i
	}

	for i, node := range nodes {
		estimator, ok := q.incomingRates[node]
		if !ok {
			continue
		}
		builder.WriteString(fmt.Sprintf("    ingress -> %d [label=\"%.2f req/s\"];\n",
			i, estimator.Estimate))
	}

	incomingRequests := q.incomingRequests()
	for i, from := range nodes {
		for j, to := range nodes {
			weight, ok := q.network[to][from]
			if !ok {
				continue
			}

			builder.WriteString(fmt.Sprintf("    %d -> %d [label=\"%.2f\"];\n",
				i, j, float64(weight)/float64(incomingRequests[from])))
		}
	}

	builder.WriteString("}")

	return builder.String()
}
