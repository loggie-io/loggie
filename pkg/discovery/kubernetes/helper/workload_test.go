package helper

import (
	"fmt"
	"reflect"
	"testing"

	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_getWorkload(t *testing.T) {
	tests := []struct {
		args *coreV1.Pod
		want Workload
	}{
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "Job", Name: "jobName"}},
				},
			},
			want: Workload{Kind: "Job", Name: "jobName"},
		},
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "ReplicaSet", Name: "rsName"}},
				},
			},
			want: Workload{Kind: "ReplicaSet", Name: "rsName"},
		},
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "ReplicaSet", Name: "dmName-c697f7b44"}},
				},
			},
			want: Workload{Kind: "Deployment", Name: "dmName"},
		},
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "StatefulSet", Name: "ssName"}},
				},
			},
			want: Workload{Kind: "StatefulSet", Name: "ssName"},
		},
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "DaemonSet", Name: "dsName"}},
				},
			},
			want: Workload{Kind: "DaemonSet", Name: "dsName"},
		},
		{
			args: &coreV1.Pod{
				ObjectMeta: metaV1.ObjectMeta{
					Name:            "podName",
					OwnerReferences: []metaV1.OwnerReference{{Kind: "ConfigMap", Name: "cmName"}},
				},
			},
			want: Workload{Kind: "Pod", Name: "podName"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("case index %d", i), func(t *testing.T) {
			if got := GetWorkload(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetWorkload() = %v, want %v", got, tt.want)
			}
		})
	}
}
