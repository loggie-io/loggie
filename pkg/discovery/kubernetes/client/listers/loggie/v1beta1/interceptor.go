/*
Copyright The Kubernetes Authors.

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
// Code generated by lister-gen. DO NOT EDIT.

package v1beta1

import (
	v1beta1 "github.com/loggie-io/loggie/pkg/discovery/kubernetes/apis/loggie/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// InterceptorLister helps list Interceptors.
// All objects returned here must be treated as read-only.
type InterceptorLister interface {
	// List lists all Interceptors in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1beta1.Interceptor, err error)
	// Get retrieves the Interceptor from the index for a given name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1beta1.Interceptor, error)
	InterceptorListerExpansion
}

// interceptorLister implements the InterceptorLister interface.
type interceptorLister struct {
	indexer cache.Indexer
}

// NewInterceptorLister returns a new InterceptorLister.
func NewInterceptorLister(indexer cache.Indexer) InterceptorLister {
	return &interceptorLister{indexer: indexer}
}

// List lists all Interceptors in the indexer.
func (s *interceptorLister) List(selector labels.Selector) (ret []*v1beta1.Interceptor, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1beta1.Interceptor))
	})
	return ret, err
}

// Get retrieves the Interceptor from the index for a given name.
func (s *interceptorLister) Get(name string) (*v1beta1.Interceptor, error) {
	obj, exists, err := s.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1beta1.Resource("interceptor"), name)
	}
	return obj.(*v1beta1.Interceptor), nil
}
