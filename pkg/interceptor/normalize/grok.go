package normalize

import (
	"bufio"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/loggie-io/loggie/pkg/util/runtime"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
)

const ProcessorGrok = "grok"

type GrokProcessor struct {
	config *GrokConfig
	groks  []*Grok
}

type Grok struct {
	p           *regexp.Regexp
	subexpNames []string
	ignoreBlank bool

	patterns     map[string]string
	patternPaths []string
}

type GrokConfig struct {
	Target       string            `yaml:"target,omitempty" default:"body"`
	Dst          string            `yaml:"dst,omitempty"`
	Match        []string          `yaml:"match,omitempty" validate:"required"`
	IgnoreBlank  bool              `yaml:"ignore_blank"`
	PatternPaths []string          `yaml:"pattern_paths,omitempty"`
	Overwrite    bool              `yaml:"overwrite"`
	IgnoreError  bool              `yaml:"ignoreError"`
	Pattern      map[string]string `yaml:"pattern,omitempty"`
}

func init() {
	register(ProcessorGrok, func() Processor {
		return NewgrokProcessor()
	})
}

func NewgrokProcessor() *GrokProcessor {
	return &GrokProcessor{
		config: &GrokConfig{},
	}
}

func (r *GrokProcessor) Config() interface{} {
	return r.config
}

func (r *GrokProcessor) Init() {
	groks := make([]*Grok, 0)
	for _, rule := range r.config.Match {
		groks = append(groks, NewGrok(rule, r.config.PatternPaths, r.config.IgnoreBlank))
	}
	r.groks = groks
}

func (r *GrokProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	target := r.config.Target
	obj := runtime.NewObject(header)

	var val string
	if target == event.Body {
		val = util.ByteToStringUnsafe(e.Body())
	} else {
		t, err := obj.GetPath(target).String()
		if err != nil {
			log.Debug("grok failed event: %s", e.String())
			return nil
		}
		if t == "" {
			log.Debug("grok failed event: %s", e.String())
			return nil
		}
		val = t
	}

	for _, grok := range r.groks {
		rst := grok.grok(val)
		if len(rst) == 0 {
			continue
		}
		if r.config.Dst == "" {
			if r.config.Overwrite {
				for field, value := range rst {
					obj.Set(field, value)
				}
			} else {
				for field, value := range rst {
					if o := obj.Get(field); o.IsNull() {
						obj.Set(field, value)
					}
				}
			}
		} else {
			dst := make(map[string]interface{})
			for field, value := range rst {
				dst[field] = value
			}
			if r.config.Overwrite {
				obj.Set(r.config.Dst, dst)
			} else {
				if o := obj.Get(r.config.Dst); o.IsNull() {
					obj.Set(r.config.Dst, dst)
				}
			}
		}
	}
	return nil
}

func NewGrok(match string, patternPaths []string, ignoreBlank bool) *Grok {
	grok := &Grok{
		patternPaths: patternPaths,
		patterns:     make(map[string]string),
		ignoreBlank:  ignoreBlank,
	}
	grok.loadPatterns()
	finalPattern := grok.translateMatchPattern(match)
	p, err := regexp.Compile(finalPattern)
	if err != nil {
		log.Error("could not build Grok:%s", err)
	}
	grok.p = p
	grok.subexpNames = p.SubexpNames()

	return grok
}

func (grok *Grok) grok(input string) map[string]string {
	rst := make(map[string]string)
	for i, substring := range grok.p.FindStringSubmatch(input) {
		if grok.subexpNames[i] == "" {
			continue
		}
		if grok.ignoreBlank && substring == "" {
			continue
		}
		rst[grok.subexpNames[i]] = substring
	}
	return rst
}

func (grok *Grok) loadPatterns() {
	for _, patternPath := range grok.patternPaths {
		var r *bufio.Reader
		if strings.HasPrefix(patternPath, "http://") || strings.HasPrefix(patternPath, "https://") {
			resp, err := http.Get(patternPath)
			if err != nil {
				log.Error("load pattern error:%s", err)
				continue
			}
			defer resp.Body.Close()
			r = bufio.NewReader(resp.Body)
			grok.parseLine(r)
		} else {
			files, err := getFiles(patternPath)
			if err != nil {
				log.Error("get files error %v", err)
			}
			for _, file := range files {
				f, err := os.Open(file)
				if err != nil {
					log.Error("load pattern error:%s", err)
				}
				r = bufio.NewReader(f)
				grok.parseLine(r)
			}
		}
	}
}

func (grok *Grok) parseLine(r *bufio.Reader) {
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error("read patterns error:%s", err)
			continue
		}
		if isPrefix == true {
			log.Error("readline prefix")
			continue
		}
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		kv := strings.SplitN(string(line), " ", 2)
		if len(kv) != 2 {
			log.Error("wrong line format : %v", string(line))
			continue
		}
		grok.patterns[kv[0]] = kv[1]
	}
}

func getFiles(filepath string) ([]string, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return []string{filepath}, nil
	}

	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	list, err := f.Readdir(-1)
	f.Close()

	if err != nil {
		return nil, err
	}
	files := make([]string, 0)
	for _, l := range list {
		if l.Mode().IsRegular() {
			files = append(files, path.Join(filepath, l.Name()))
		}
	}
	return files, nil
}

func (grok *Grok) translateMatchPattern(s string) string {
	p, err := regexp.Compile(`%{\w+?(:\w+?)?}`)
	if err != nil {
		log.Error("could not translate match pattern: %v", err.Error())
		return ""
	}
	var r string
	for {
		r = p.ReplaceAllStringFunc(s, grok.replaceFunc)
		if r == s {
			return r
		}
		s = r
	}
}

func (grok *Grok) replaceFunc(s string) string {
	p, err := regexp.Compile(`%{(\w+?)(?::(\w+?))?}`)
	if err != nil {
		log.Error("could not replace match func: %v", err.Error())
		return ""
	}
	rst := p.FindAllStringSubmatch(s, -1)
	if len(rst) != 1 {
		log.Error("sub match in `%s` != 1", s)
		return ""
	}
	if pattern, ok := grok.patterns[rst[0][1]]; ok {
		if rst[0][2] == "" {
			return fmt.Sprintf("(%s)", pattern)
		} else {
			return fmt.Sprintf("(?P<%s>%s)", rst[0][2], pattern)
		}
	} else {
		log.Error("`%s` could not be found", rst[0][1])
		return ""
	}
}
