package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/interceptor"
	"github.com/loggie-io/loggie/pkg/core/queue"
	"github.com/loggie-io/loggie/pkg/core/sink"
	"github.com/loggie-io/loggie/pkg/core/source"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/clientset/versioned/fake"
	lgcInformer "github.com/loggie-io/loggie/pkg/discovery/kubernetes/client/informers/externalversions"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

func TestGetConfigFromPodAndLogConfig(t *testing.T) {

	ctrl := &Controller{
		config: &Config{
			PodLogDirPrefix: "/var/log/pods",
			K8sFields: map[string]string{
				"namespace":     "${_k8s.pod.namespace}",
				"podname":       "${_k8s.pod.name}",
				"containername": "${_k8s.pod.container.name}",
				"nodename":      "${_k8s.node.name}",
				"logconfig":     "${_k8s.logconfig}",
			},
		},
	}
	ctrl.InitK8sFieldsPattern()

	lgc := logconfigv1beta1.LogConfig{
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
			Pipeline: &logconfigv1beta1.Pipeline{
				Sources: `
      - type: file
        containerName: nginx
        name: common
        paths:
          - stdout
        matchFields:
          env: ["SVC"]
`,
				Interceptors: `
      - type: normalize
        belongTo: ["common"]
`,
				Sink: `
      type: dev
      printEvents: false
`,
				Queue: `
      type: channel
      name: queue
      batchSize: 1024
`,
			},
		},
	}

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nginx-79f66cb89d-99gxd",
			Namespace: "default",
			Labels: map[string]string{
				"app": "nginx",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "nginx",
					Env: []corev1.EnvVar{
						{
							Name:  "SVC",
							Value: "NET",
						},
					},
				},
			},
			NodeName: "kind-control-plane",
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "nginx",
					ContainerID: "containerd://b9fea27064965c8fdf80d4b852545b2fe0b469073582765e11b1acda3691293a",
				},
			},
		},
	}

	clientset := fake.NewSimpleClientset()
	lgcInf := lgcInformer.NewSharedInformerFactory(clientset, 0)

	want := &pipeline.Config{
		Name: "default/testnginx",
		Sources: []*source.Config{
			{
				Name: "nginx-79f66cb89d-99gxd/nginx/common",
				Type: "file",
				Properties: cfg.CommonCfg{
					"paths": []interface{}{
						"/var/log/pods/nginx/*.log",
						"/var/log/pods/default_nginx-79f66cb89d-99gxd_/nginx/*.log",
					},
				},
				Fields: map[string]interface{}{
					"containername": "nginx",
					"logconfig":     "testnginx",
					"namespace":     "default",
					"nodename":      "kind-control-plane",
					"podname":       "nginx-79f66cb89d-99gxd",
					"SVC":           "NET",
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
					"belongTo": []string{"nginx-79f66cb89d-99gxd/nginx/common"},
				},
			},
		},
		Queue: &queue.Config{
			Enabled:    nil,
			Name:       "queue",
			Type:       "channel",
			Properties: nil,
			BatchSize:  1024,
		},
	}

	got, err := ctrl.getConfigFromPodAndLogConfig(&lgc, &pod, lgcInf.Loggie().V1beta1().Sinks().Lister(), lgcInf.Loggie().V1beta1().Interceptors().Lister())
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func Test_regexAndReplace(t *testing.T) {
	type args struct {
		in      map[string]string
		regex   string
		replace string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "ok",
			args: args{
				in: map[string]string{
					"a":          "b",
					"foo.bar/cc": "dd",
				},
				regex:   "foo.bar/(.*)",
				replace: "pre-${1}",
			},
			want: map[string]string{
				"pre-cc": "dd",
			},
		},
		{
			name: "prefix",
			args: args{
				in: map[string]string{
					"a": "b",
					"c": "d",
				},
				regex:   "(.*)",
				replace: "pre-${1}",
			},
			want: map[string]string{
				"pre-a": "b",
				"pre-c": "d",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := regexAndReplace(tt.args.in, tt.args.regex, tt.args.replace)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
