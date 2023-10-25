package helper

import (
	"context"
	"github.com/loggie-io/loggie/pkg/core/log"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
)

type workloadFilterInfo struct {
	// 命名空间
	namespaces map[string]struct{}
	// 排除的命名空间
	excludeNamespaces map[string]struct{}
	// 负载名字
	names map[string]struct{}
}

type filterCacheChecker struct {
	namespaces        map[string]string
	excludeNamespaces map[string]string
	// workload(type) => workloadFilterInfo
	workloadSelector map[string]workloadFilterInfo
	clientSet        kubeclientset.Interface
	lgc              *logconfigv1beta1.LogConfig
}

func newFilterCacheChecker(lgc *logconfigv1beta1.LogConfig, clientSet kubeclientset.Interface) *filterCacheChecker {
	f := &filterCacheChecker{
		clientSet: clientSet,
		lgc:       lgc,
	}
	if lgc.Spec.Selector == nil {
		return f
	}

	if len(lgc.Spec.Selector.NamespaceSelector.NamespaceSelector) != 0 {
		f.namespaces = make(map[string]string)
		for _, v := range lgc.Spec.Selector.NamespaceSelector.NamespaceSelector {
			f.namespaces[v] = v
		}
	}

	if len(lgc.Spec.Selector.NamespaceSelector.ExcludeNamespaceSelector) != 0 {
		f.excludeNamespaces = make(map[string]string)
		for _, v := range lgc.Spec.Selector.NamespaceSelector.ExcludeNamespaceSelector {
			f.excludeNamespaces[v] = v
		}
	}

	if len(lgc.Spec.Selector.NamespaceSelector.ExcludeNamespaceSelector) != 0 {
		f.excludeNamespaces = make(map[string]string)
		for _, v := range lgc.Spec.Selector.NamespaceSelector.ExcludeNamespaceSelector {
			f.excludeNamespaces[v] = v
		}
	}

	if len(lgc.Spec.Selector.WorkloadSelector) != 0 {
		f.workloadSelector = make(map[string]workloadFilterInfo)
		for _, v := range lgc.Spec.Selector.WorkloadSelector {
			for _, workloadType := range v.Type {
				_, ok := f.workloadSelector[workloadType]
				if !ok {
					f.workloadSelector[workloadType] = workloadFilterInfo{
						namespaces:        make(map[string]struct{}),
						excludeNamespaces: make(map[string]struct{}),
						names:             make(map[string]struct{}),
					}
				}

				if len(v.NamespaceSelector) != 0 {
					for _, namespace := range v.NamespaceSelector {
						f.workloadSelector[workloadType].namespaces[namespace] = struct{}{}
					}
				}

				if len(v.ExcludeNamespaceSelector) != 0 {
					for _, namespace := range v.ExcludeNamespaceSelector {
						f.workloadSelector[workloadType].excludeNamespaces[namespace] = struct{}{}
					}
				}

				if len(v.NameSelector) != 0 {
					for _, name := range v.NameSelector {
						f.workloadSelector[workloadType].names[name] = struct{}{}
					}
				}
			}

		}
	}

	return f
}

// checkNamespace 检查namespace 是否合法
func (p *filterCacheChecker) checkNamespace(pod *corev1.Pod) bool {
	if len(p.namespaces) == 0 && len(p.excludeNamespaces) == 0 {
		return true
	}

	if len(p.excludeNamespaces) != 0 {
		_, ok := p.excludeNamespaces[pod.GetNamespace()]
		if ok {
			return false
		}
	}

	if len(p.namespaces) == 0 {
		return true
	}

	_, ok := p.namespaces[pod.GetNamespace()]
	if ok {
		return true
	}

	return false
}

func (p *filterCacheChecker) checkOwner(owner metav1.OwnerReference, namespace string) (bool, error) {

	// 如果没有选workloadSelector，那么默认是全部符合的
	if len(p.workloadSelector) == 0 {
		return true, nil
	}

	kind := owner.Kind
	name := owner.Name
	if owner.Kind == "ReplicaSet" {
		rs, err := p.clientSet.AppsV1().ReplicaSets(namespace).Get(context.TODO(), owner.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if len(rs.GetOwnerReferences()) == 0 {
			return false, nil
		}

		deploymentOwner := rs.GetOwnerReferences()[0]
		if deploymentOwner.Kind != "Deployment" {
			return false, nil
		}
		kind = "Deployment"
		name = deploymentOwner.Name
	}

	workloadInfo, ok := p.workloadSelector[kind]
	if !ok {
		return false, nil
	}

	if len(workloadInfo.namespaces) != 0 {
		_, ok = workloadInfo.namespaces[namespace]
		if !ok {
			return false, nil
		}
	}

	if len(workloadInfo.excludeNamespaces) != 0 {
		_, ok = workloadInfo.excludeNamespaces[namespace]
		if ok {
			return false, nil
		}
	}

	if len(workloadInfo.names) != 0 {
		_, ok = workloadInfo.names[name]
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (p *filterCacheChecker) checkWorkload(pod *corev1.Pod) bool {
	owners := pod.GetOwnerReferences()
	if len(owners) == 0 {
		return false
	}

	for _, owner := range owners {
		ret, err := p.checkOwner(owner, pod.GetNamespace())
		if err != nil {
			log.Error("check owner error:%s", err)
			return false
		}
		if !ret {
			return false
		}
	}
	return true
}

func (p *filterCacheChecker) checkLabels(pod *corev1.Pod) bool {
	if p.lgc.Spec.Selector == nil {
		return true
	}
	if len(p.lgc.Spec.Selector.LabelSelector) != 0 {
		if LabelsSubset(p.lgc.Spec.Selector.LabelSelector, pod.Labels) {
			return true
		}
		return false
	}

	return true
}
