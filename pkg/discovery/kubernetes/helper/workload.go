package helper

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Workload struct {
	Kind string
	Name string
}

func GetWorkload(pod *corev1.Pod) Workload {
	refs := pod.GetOwnerReferences()
	if len(refs) == 0 {
		return Workload{}
	}
	for _, ref := range refs {
		kind, name := parseWorkloadKindName(ref)
		if kind != "" && name != "" {
			return Workload{
				Kind: kind,
				Name: name,
			}
		}
	}
	return Workload{
		Kind: "Pod",
		Name: pod.GetName(),
	}
}

func parseWorkloadKindName(ref metav1.OwnerReference) (kind, name string) {
	switch ref.Kind {
	case "ReplicaSet":
		dmName := parseDeploymentName(ref.Name)
		if dmName == "" {
			return "ReplicaSet", ref.Name
		}
		return "Deployment", dmName
	case "StatefulSet", "DaemonSet", "Job":
		return ref.Kind, ref.Name
		// TODO: parse cronjob
	}
	return "", ""
}

func parseDeploymentName(replicaSetName string) string {
	index := strings.LastIndex(replicaSetName, "-")
	if index == -1 || !isTemplateHash(replicaSetName[index+1:]) {
		return ""
	}
	return replicaSetName[:index]
}

func isTemplateHash(s string) bool {
	// fnv sum32 return value Range: 0 through 4294967295.
	if len(s) < 1 || len(s) > 10 {
		return false
	}
	for _, b := range s {
		// b ∈ "456789bcdf"
		// see https://github.com/kubernetes/apimachinery/blob/v0.24.0/pkg/util/rand/rand.go#L121
		if (b >= '4' && b <= '9') || (b >= 'b' && b <= 'f') {
			continue
		}
		return false
	}
	return true
}
