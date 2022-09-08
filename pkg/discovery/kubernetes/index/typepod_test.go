package index

import (
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestTypePodIndex(t *testing.T) {

	index := NewLogConfigTypePodIndex()

	lgc := &logconfigv1beta1.LogConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testnginx",
			Namespace: "default",
		},
		Spec: logconfigv1beta1.Spec{
			Selector: &logconfigv1beta1.Selector{
				Type: logconfigv1beta1.SelectorTypePod,
				PodSelector: logconfigv1beta1.PodSelector{
					LabelSelector: map[string]string{
						"app": "nginx",
					},
				},
			},
		},
	}

	podA := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx-aaa",
			Namespace: "default",
			Labels: map[string]string{
				"app": "nginx",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "nginx",
				},
			},
		},
	}

	podB := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx-bbb",
			Namespace: "default",
			Labels: map[string]string{
				"app": "nginx",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "nginx",
				},
			},
		},
	}

	pipeconfigA := &pipeline.Config{
		Name: "default/testnginx",
		Sources: []*source.Config{
			{
				Name: "nginx-aaa",
				Type: "file",
				Fields: map[string]interface{}{
					"podname": "nginx-aaa",
				},
			},
		},
		Sink: &sink.Config{
			Type: "dev",
			Properties: cfg.CommonCfg{
				"printEvents": false,
			},
		},
		Interceptors: []*interceptor.Config{
			{
				Type: "normalize",
				Properties: cfg.CommonCfg{
					"belongTo": []string{"nginx-aaa"},
				},
			},
		},
	}

	pipeconfigB := &pipeline.Config{
		Name: "default/testnginx",
		Sources: []*source.Config{
			{
				Name: "nginx-bbb",
				Type: "file",
				Fields: map[string]interface{}{
					"podname": "nginx-bbb",
				},
			},
		},
		Sink: &sink.Config{
			Type: "dev",
			Properties: cfg.CommonCfg{
				"printEvents": false,
			},
		},
		Interceptors: []*interceptor.Config{
			{
				Type: "normalize",
				Properties: cfg.CommonCfg{
					"belongTo": []string{"nginx-bbb"},
				},
			},
		},
	}

	index.SetConfigs(podA.Namespace, podA.Name, lgc.Name, pipeconfigA, lgc)
	index.SetConfigs(podB.Namespace, podB.Name, lgc.Name, pipeconfigB, lgc)

	want := &control.PipelineConfig{
		Pipelines: []pipeline.Config{
			{
				Name: "default/testnginx",
				Sources: []*source.Config{
					{
						Name: "nginx-aaa",
						Type: "file",
						Fields: map[string]interface{}{
							"podname": "nginx-aaa",
						},
					},
					{
						Name: "nginx-bbb",
						Type: "file",
						Fields: map[string]interface{}{
							"podname": "nginx-bbb",
						},
					},
				},
				Sink: &sink.Config{
					Type: "dev",
					Properties: cfg.CommonCfg{
						"printEvents": false,
					},
				},
				Interceptors: []*interceptor.Config{
					{
						Type: "normalize",
						Properties: cfg.CommonCfg{
							"belongTo": []string{"nginx-aaa", "nginx-bbb"},
						},
					},
				},
			},
		},
	}
	got := index.GetAllGroupByLogConfig(false)
	assert.Equal(t, want, got)
}
