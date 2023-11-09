package helper

import (
	"errors"
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	corev1 "k8s.io/api/core/v1"
	kubeclientset "k8s.io/client-go/kubernetes"
)

// PodsConfirm 检查pod是否符合logConfig的规则
type PodsConfirm struct {
	lgc       *logconfigv1beta1.LogConfig
	clientSet kubeclientset.Interface
	cache     *filterCacheChecker
}

func NewPodsConfirm(lgc *logconfigv1beta1.LogConfig, clientSet kubeclientset.Interface) *PodsConfirm {
	return &PodsConfirm{
		lgc:       lgc,
		clientSet: clientSet,
		cache:     newFilterCacheChecker(lgc, clientSet),
	}
}

// Confirm Confirm whether the pod meets the lgc rules
func (p *PodsConfirm) Confirm(pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, errors.New("confirm pod error;pod is nil")
	}

	if !IsPodReady(pod) {
		return false, nil
	}

	// check label
	if !p.cache.checkLabels(pod) {
		return false, nil
	}

	if !p.cache.checkNamespace(pod) {
		return false, nil
	}

	if !p.cache.checkWorkload(pod) {
		return false, nil
	}

	return true, nil
}
