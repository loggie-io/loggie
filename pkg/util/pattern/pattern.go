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
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"github.com/loggie-io/loggie/pkg/util/time"
	"github.com/pkg/errors"
	"os"
	"regexp"
	"strings"
)

const (
	Indicator       = '$'
	SeparatorPrefix = '{'
	SeparatorSuffix = '}'
	matchExpr       = `\${(.+?)}`

	timeToken = "+"
	envToken  = "_env."

	kindTime   = "time"
	kindEnv    = "env"
	kindK8s    = "k8s"
	kindObject = "object"
)

var ErrEmptyMatcher = errors.New("render matcher is empty")

type Pattern struct {
	Raw            string
	isConstVal     bool
	matcher        []matcher
	tmpObj         *runtime.Object
	tmpK8sPodData  *TypePodFieldsData
	tmpK8sNodeData *TypeNodeFieldsData
}

type matcher struct {
	keyWrap string // e.g. ${fields.xx}
	key     string // e.g. fields.xx
	kind    string
}

// EnvMatcher matches env var, e.g. ${_env.XXX}
func isEnvVar(key string) bool {
	return strings.HasPrefix(key, envToken)
}
func envMatcherRender(key string) string {
	ev := strings.TrimPrefix(key, envToken)
	return os.Getenv(ev)
}

// TimeMatcher matches date var, e.g. ${+YYYY.MM.dd}
func isTimeVar(key string) bool {
	return strings.HasPrefix(key, timeToken)
}
func timeMatcherRender(key string) string {
	return time.TimeFormatNow(strings.TrimPrefix(key, timeToken))
}

// ObjectMatcher retrieve any fields from events, e.g. ${a.b}
func objectMatcherRender(obj *runtime.Object, key string) (string, error) {
	if obj == nil {
		return "", nil
	}

	val, err := obj.GetPath(key).String()
	if err != nil {
		return "", err
	}
	return val, nil
}

func Validate(pattern string) error {
	_, err := Init(pattern)
	return err
}

func Init(pattern string) (*Pattern, error) {
	reg, err := regexp.Compile(matchExpr)
	if err != nil {
		return nil, err
	}

	var matcher []matcher
	match := reg.FindAllStringSubmatch(pattern, -1)
	for _, m := range match {
		matcher = append(matcher, makeMatch(m))
	}

	isConstVal := false
	if len(match) == 0 {
		isConstVal = true
	}

	return &Pattern{
		Raw:        pattern,
		matcher:    matcher,
		isConstVal: isConstVal,
	}, nil
}

func MustInit(pattern string) *Pattern {
	p, err := Init(pattern)
	if err != nil {
		panic(err)
	}
	return p
}

func makeMatch(m []string) matcher {
	keyWrap := m[0]
	key := m[1]
	item := matcher{
		keyWrap: keyWrap,
		key:     key,
	}

	if isEnvVar(key) {
		item.kind = kindEnv
	} else if isTimeVar(key) {
		item.kind = kindTime
	} else if IsK8sVar(key) {
		item.kind = kindK8s
	} else {
		item.kind = kindObject
	}
	return item
}

// RenderWithStrict any placeholder rendering empty will return an error
func (p *Pattern) RenderWithStrict() (string, error) {
	return p.render(true)
}

func (p *Pattern) Render() (string, error) {
	return p.render(false)
}

// Render to actual results based on placeholders
// If `strict` is set to true, any placeholder rendering empty will return an error.
func (p *Pattern) render(strict bool) (string, error) {
	if p.isConstVal || len(p.matcher) == 0 {
		return p.Raw, nil
	}

	var oldNew []string
	for _, m := range p.matcher {

		var alt string
		if m.kind == kindEnv {
			alt = envMatcherRender(m.key)
		} else if m.kind == kindTime {
			alt = timeMatcherRender(m.key)
		} else if m.kind == kindObject {
			o, err := objectMatcherRender(p.tmpObj, m.key)
			if err != nil {
				return "", err
			}
			alt = o
		} else if m.kind == kindK8s {
			alt = p.K8sMatcherRender(m.key)
		}

		if alt == "" && strict {
			return "", errors.WithMessagef(ErrEmptyMatcher, "with %s", m.keyWrap)
		}

		// add old
		oldNew = append(oldNew, m.keyWrap)
		// add new
		oldNew = append(oldNew, alt)
	}

	replacer := strings.NewReplacer(oldNew...)
	return replacer.Replace(p.Raw), nil
}

func (p *Pattern) WithObject(obj *runtime.Object) *Pattern {
	p.tmpObj = obj
	return p
}

func (p *Pattern) WithK8sPod(data *TypePodFieldsData) *Pattern {
	p.tmpK8sPodData = data
	return p
}

func (p *Pattern) WithK8sNode(data *TypeNodeFieldsData) *Pattern {
	p.tmpK8sNodeData = data
	return p
}

// GetSplits
// eg: target="/var/log/${pod.uid}/${pod.name}/"
//     returns ["var/log/", "/", "/"] and ["pod.uid", "pod.name"]
func GetSplits(target string) (splitStr []string, matchers []string) {

	var splitStrList []string
	var matcherList []string

	inMatcher := false
	var splitStrBuilder strings.Builder
	var matcherBuilder strings.Builder

	for i, s := range target {
		if !inMatcher {
			if s == Indicator && i < len(target)-1 && target[i+1] == SeparatorPrefix { // not the last rune, and the next is '{'
				if splitStrBuilder.Len() > 0 {
					splitStrList = append(splitStrList, splitStrBuilder.String())
					splitStrBuilder.Reset()
				}
				inMatcher = true
				continue
			}

			splitStrBuilder.WriteRune(s)
			continue
		}

		// in match record
		if s == Indicator || s == SeparatorPrefix { // ignore
			continue
		}

		if s == SeparatorSuffix { // end the matcher
			if matcherBuilder.Len() > 0 {
				matcherList = append(matcherList, matcherBuilder.String())
				matcherBuilder.Reset()
			}
			inMatcher = false
			continue
		}

		matcherBuilder.WriteRune(s)
	}

	if splitStrBuilder.Len() > 0 {
		splitStrList = append(splitStrList, splitStrBuilder.String())
	}
	return splitStrList, matcherList
}

// Extract
// eg: input="/var/log/76fb94cbb5/tomcat/", splitsStr=["/var/log/", "/", "/"]
//     return ["76fb94cbb5", "tomcat"]
func Extract(input string, splitsStr []string) []string {
	var ret []string
	segment := input
	for _, split := range splitsStr {
		lastIndex := strings.Index(segment, split) + len(split)
		sub := segment[:lastIndex]
		segment = segment[lastIndex:]
		val := strings.TrimSuffix(sub, split)
		if val != "" {
			ret = append(ret, val)
		}
	}

	return ret
}
