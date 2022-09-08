/*
Copyright 2022 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pattern

import (
	"fmt"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/loggie-io/loggie/pkg/util/time"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestExtract(t *testing.T) {
	type args struct {
		input     string
		splitsStr []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "ok-1",
			args: args{
				input: "/var/d3/dade_svc-adx/sa.log",
				splitsStr: []string{
					"/var/d3/", "_", "/",
				},
			},
			want: []string{
				"dade", "svc-adx",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Extract(tt.args.input, tt.args.splitsStr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Extract() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSplits(t *testing.T) {
	type args struct {
		target string
	}
	tests := []struct {
		name         string
		args         args
		wantSplitStr []string
		wantMatchers []string
	}{
		{
			name: "ok-1",
			args: args{
				target: "${name}/${uid}_${ns}",
			},
			wantSplitStr: []string{
				"/", "_",
			},
			wantMatchers: []string{
				"name", "uid", "ns",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSplitStr, gotMatchers := GetSplits(tt.args.target)
			if !reflect.DeepEqual(gotSplitStr, tt.wantSplitStr) {
				t.Errorf("GetSplits() gotSplitStr = %v, want %v", gotSplitStr, tt.wantSplitStr)
			}
			if !reflect.DeepEqual(gotMatchers, tt.wantMatchers) {
				t.Errorf("GetSplits() gotMatchers = %v, want %v", gotMatchers, tt.wantMatchers)
			}
		})
	}
}

func TestEnvPattern(t *testing.T) {
	envs := strings.Split(os.Environ()[0], "=")
	envKey := envs[0]
	envVal := envs[1]

	type args struct {
		pattern string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "empty string",
			args: args{
				pattern: "",
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "const value",
			args: args{
				pattern: "constValue",
			},
			want:    "constValue",
			wantErr: false,
		},
		{
			name: "got env",
			args: args{
				pattern: fmt.Sprintf("pre-${_env.%s}", envKey),
			},
			want:    "pre-" + envVal,
			wantErr: false,
		},
		{
			name: "got time",
			args: args{
				pattern: "pre-${+YYYY.MM.dd}",
			},
			want:    "pre-" + time.TimeFormatNow("YYYY.MM.dd"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Init(tt.args.pattern)
			if err != nil {
				t.Errorf("init pattern error: %v", err)
			}

			got, err := p.Render()
			if (err != nil) != tt.wantErr {
				t.Errorf("Render() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Render() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObjectPattern(t *testing.T) {
	type args struct {
		pattern string
		obj     *runtime.Object
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "got object fields",
			args: args{
				pattern: "${a.b}",
				obj: runtime.NewObject(map[string]interface{}{
					"a": map[string]interface{}{
						"b": "c",
					}}),
			},
			want:    "c",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Init(tt.args.pattern)
			if err != nil {
				t.Errorf("init pattern error: %v", err)
			}

			got, err := p.WithObject(tt.args.obj).Render()
			if (err != nil) != tt.wantErr {
				t.Errorf("Render() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Render() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestK8sPattern(t *testing.T) {

	testpod := &corev1.Pod{}
	testpod.Namespace = "ns1"
	testpod.Name = "tomcatpod"
	testpod.Spec = corev1.PodSpec{
		NodeName: "node1",
	}
	testpod.Status = corev1.PodStatus{
		PodIP:  "123.123.123.123",
		HostIP: "111.111.111.111",
	}

	testdata := &TypePodFieldsData{
		ContainerName: "tomcat",
		LogConfig:     "testlgc",
		Pod:           testpod,
	}

	type args struct {
		pattern string
		data    *TypePodFieldsData
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "got pod name",
			args: args{
				pattern: "${_k8s.pod.name}",
				data:    testdata,
			},
			want: "tomcatpod",
		},
		{
			name: "got namespace",
			args: args{
				pattern: "${_k8s.pod.namespace}",
				data:    testdata,
			},
			want: "ns1",
		},
		{
			name: "got pod ip",
			args: args{
				pattern: "${_k8s.pod.ip}",
				data:    testdata,
			},
			want: "123.123.123.123",
		},
		{
			name: "got container name",
			args: args{
				pattern: "${_k8s.pod.container.name}",
				data:    testdata,
			},
			want: "tomcat",
		},
		{
			name: "got node name",
			args: args{
				pattern: "${_k8s.node.name}",
				data:    testdata,
			},
			want: "node1",
		},
		{
			name: "got node ip",
			args: args{
				pattern: "${_k8s.node.ip}",
				data:    testdata,
			},
			want: "111.111.111.111",
		},
		{
			name: "got logconfig",
			args: args{
				pattern: "${_k8s.logconfig}",
				data:    testdata,
			},
			want: "testlgc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Init(tt.args.pattern)
			if err != nil {
				t.Errorf("init pattern error: %v", err)
			}

			got, err := p.WithK8sPod(testdata).Render()
			if (err != nil) != tt.wantErr {
				t.Errorf("Render() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Render() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestK8sPatternTypeNode(t *testing.T) {
	testnode := &corev1.Node{}
	testnode.Name = "node1"
	testnode.Labels = make(map[string]string)
	testnode.Labels["kubernetes.io/os"] = "linux"
	testnode.Annotations = make(map[string]string)
	testnode.Annotations["kubernetes.io/arch"] = "amd64"
	testnode.Status = corev1.NodeStatus{
		Addresses: []corev1.NodeAddress{
			{
				Type:    corev1.NodeInternalIP,
				Address: "127.0.0.1",
			},
			{
				Type:    corev1.NodeHostName,
				Address: "a.b.c",
			},
		},
		NodeInfo: corev1.NodeSystemInfo{
			OSImage: "CentOS Linux 7 (Core)",
		},
	}

	testdata := &TypeNodeFieldsData{
		Node:             testnode,
		ClusterLogConfig: "test",
	}

	type args struct {
		pattern string
		data    *TypeNodeFieldsData
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "got node name",
			args: args{
				pattern: "${_k8s.node.name}",
				data:    testdata,
			},
			want: "node1",
		},
		{
			name: "got clusterlogconfig",
			args: args{
				pattern: "${_k8s.clusterlogconfig}",
				data:    testdata,
			},
			want: "test",
		},
		{
			name: "got node.addresses.InternalIP",
			args: args{
				pattern: "${_k8s.node.addresses.InternalIP}",
				data:    testdata,
			},
			want: "127.0.0.1",
		},
		{
			name: "got nodeInfo osImage",
			args: args{
				pattern: "${_k8s.node.nodeInfo.osImage}",
				data:    testdata,
			},
			want: "CentOS Linux 7 (Core)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := Init(tt.args.pattern)
			if err != nil {
				t.Errorf("init pattern error: %v", err)
			}

			got, err := p.WithK8sNode(testdata).Render()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
