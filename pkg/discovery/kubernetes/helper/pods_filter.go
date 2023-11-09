package helper

import (
	logconfigv1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubeclientset "k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

type PodsFilter struct {
	podsLister corev1listers.PodLister
	lgc        *logconfigv1beta1.LogConfig
	clientSet  kubeclientset.Interface
	cache      *filterCacheChecker
}

func NewPodFilter(lgc *logconfigv1beta1.LogConfig, podsLister corev1listers.PodLister, clientSet kubeclientset.Interface) *PodsFilter {
	p := &PodsFilter{
		lgc:        lgc,
		clientSet:  clientSet,
		podsLister: podsLister,
		cache:      newFilterCacheChecker(lgc, clientSet),
	}

	return p
}

func (p *PodsFilter) getLabelSelector(lgc *logconfigv1beta1.LogConfig) (labels.Selector, error) {
	var matchExpressions []metav1.LabelSelectorRequirement
	for key, val := range lgc.Spec.Selector.LabelSelector {
		if val != MatchAllToken {
			continue
		}
		sel := metav1.LabelSelectorRequirement{
			Key:      key,
			Operator: metav1.LabelSelectorOpExists,
		}
		matchExpressions = append(matchExpressions, sel)
	}

	for k, v := range lgc.Spec.Selector.LabelSelector {
		if v == MatchAllToken {
			delete(lgc.Spec.Selector.LabelSelector, k)
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels:      lgc.Spec.Selector.LabelSelector,
		MatchExpressions: matchExpressions,
	})
	if err != nil {
		return nil, errors.WithMessagef(err, "make LabelSelector error")
	}
	return selector, nil
}

// GetPodsByLabelSelector select pod by label
func (p *PodsFilter) getPodsByLabelSelector() ([]*corev1.Pod, error) {
	// By default read all
	if p.lgc.Spec.Selector == nil || (len(p.lgc.Spec.Selector.PodSelector.LabelSelector) == 0) {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{})
		if err != nil {
			return nil, errors.WithMessagef(err, "make LabelSelector error")
		}
		ret, err := p.podsLister.List(selector)
		if err != nil {
			return nil, errors.WithMessagef(err, "%s/%s cannot find pod by labelSelector %#v", p.lgc.Namespace, p.lgc.Name, p.lgc.Spec.Selector.PodSelector.LabelSelector)
		}
		return ret, nil
	}

	// Prefer labelSelector
	labelSelectors, err := p.getLabelSelector(p.lgc)
	if err != nil {
		return nil, err
	}
	ret, err := p.podsLister.List(labelSelectors)
	if err != nil {
		return nil, errors.WithMessagef(err, "%s/%s cannot find pod by labelSelector %#v", p.lgc.Namespace, p.lgc.Name, p.lgc.Spec.Selector.PodSelector.LabelSelector)
	}
	return ret, nil
}

// Filter Filter pods
func (p *PodsFilter) Filter() ([]*corev1.Pod, error) {
	pods, err := p.getPodsByLabelSelector()

	if err != nil {
		return nil, err
	}

	if len(p.cache.namespaces) == 0 && len(p.cache.excludeNamespaces) == 0 && len(p.cache.workloadSelector) == 0 {
		return pods, nil
	}

	result := make([]*corev1.Pod, 0)

	for _, pod := range pods {

		if !IsPodReady(pod) {
			continue
		}

		if !p.cache.checkNamespace(pod) {
			continue
		}

		if !p.cache.checkWorkload(pod) {
			continue
		}

		result = append(result, pod)
	}

	return result, nil
}
