package controller

import (
	"context"
	"fmt"
	"log"
	"math"

	"github.com/pako-23/queue-scaler/internal/queue"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	cliv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"
)

const (
	scaleUpsThreshold         = 0
	scaleDownsThreshold       = 30
	maxReplicas         int32 = 20
	minReplicas         int32 = 1
)

type deployment struct {
	replicas    int32
	scaleUps    int
	scaledDowns int
}

type KubeController struct {
	client      cliv1.DeploymentInterface
	state       map[string]*deployment
	minReplicas int32
	maxReplicas int32
}

func NewKubeController() (*KubeController, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	controller := &KubeController{
		client:      clientset.AppsV1().Deployments(apiv1.NamespaceDefault),
		maxReplicas: maxReplicas,
		minReplicas: minReplicas,
	}

	if err := controller.updateState(); err != nil {
		return nil, err
	}
	log.Printf("initial controller state is: %v\n", controller.state)

	return controller, nil

}

func (k *KubeController) updateState() error {
	deployments, err := k.client.List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	k.state = make(map[string]*deployment, len(deployments.Items))

	for _, deploy := range deployments.Items {
		if value, ok := deploy.Annotations["queue-scaler"]; ok && value == "no-scale" {
			continue
		}

		k.state[deploy.Name] = &deployment{
			replicas:    *deploy.Spec.Replicas,
			scaledDowns: 0,
		}

	}

	return nil
}

func (k *KubeController) replicas(service string, incomingRate float64, serviceRate float64) int32 {
	if incomingRate == 0.0 {
		return k.minReplicas
	}

	replicas := int32(math.Ceil(incomingRate / (0.9 * serviceRate)))
	if replicas > k.maxReplicas {
		return k.maxReplicas
	}

	return replicas
}

func (k *KubeController) Stabilize(state *queue.QueueNetwork) error {
	incomingRates := state.IncomingRates()

	for service, deploy := range k.state {
		rate, ok := incomingRates[service]
		if !ok {
			continue
		}

		expectedReplicas := k.replicas(service, rate, state.NodeMetrics[service].ServiceRate())
		if deploy.replicas == expectedReplicas {
			deploy.scaledDowns = 0
			deploy.scaleUps = 0
		} else if deploy.replicas > expectedReplicas && deploy.scaledDowns < scaleDownsThreshold {
			deploy.scaledDowns += 1
		} else if deploy.replicas < expectedReplicas && deploy.scaleUps < scaleUpsThreshold {
			deploy.scaleUps += 1
		} else {
			patch := []byte(fmt.Sprintf("{\"spec\": {\"replicas\": %d}}", expectedReplicas))
			out, err := k.client.Patch(context.Background(),
				service, types.StrategicMergePatchType,
				patch, metav1.PatchOptions{})
			if err != nil {
				return err
			}

			log.Println(state.ToDOT())
			log.Printf("changed replicas for service '%s': %d -> %d\n",
				service, deploy.replicas, expectedReplicas)
			deploy.scaleUps = 0
			deploy.scaledDowns = 0
			deploy.replicas = *out.Spec.Replicas
		}
	}

	return nil
}
