/*
Copyright 2021 Loggie Authors

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

package util

import (
	"bytes"
	"fmt"
	"loggie.io/loggie/pkg/core/log"
	"regexp"
	"regexp/syntax"
	"strings"
)

type trans func(*syntax.Regexp) (bool, *syntax.Regexp)

var transformations = []trans{
	simplify,
	uncapture,
	trimLeft,
	trimRight,
	unconcat,
	concatRepetition,
	flattenRepetition,
}

// common predefined patterns
var (
	patDotStar          = mustParse(`.*`)
	patNullBeginDotStar = mustParse(`^.*`)
	patNullEndDotStar   = mustParse(`.*$`)

	patEmptyText      = mustParse(`^$`)
	patEmptyWhiteText = mustParse(`^\s*$`)

	// patterns matching any content
	patAny1 = patDotStar
	patAny2 = mustParse(`^.*`)
	patAny3 = mustParse(`^.*$`)
	patAny4 = mustParse(`.*$`)

	patDigits = mustParse(`\d`)
)

func mustParse(pattern string) *syntax.Regexp {
	r, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		panic(err)
	}
	return r
}

type Matcher struct {
	stringMatcher
}
type stringMatcher interface {
	// MatchString tries to find a matching substring.
	MatchString(s string) (matched bool)

	// Match tries to find a matching substring.
	Match(bs []byte) (matched bool)

	// String describe the generator
	String() string
}

type equalsMatcher struct {
	s  string
	bs []byte
}

type substringMatcher struct {
	s  string
	bs []byte
}

type altSubstringMatcher struct {
	literals [][]byte
}

type oneOfMatcher struct {
	literals [][]byte
}

type prefixMatcher struct {
	s []byte
}

type altPrefixMatcher struct {
	literals [][]byte
}

type prefixNumDate struct {
	minLen int
	prefix []byte
	digits []int
	seps   [][]byte
	suffix []byte
}

type emptyStringMatcher struct{}

type emptyWhiteStringMatcher struct{}

type matchAny struct{}

func MustCompile(pattern string) Matcher {
	m, err := Compile(pattern)
	if err != nil {
		log.Error("matcher compile failed!")
		panic(err)
	}
	return m
}

func Compile(pattern string) (Matcher, error) {
	regex, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return Matcher{}, err
	}

	regex = optimize(regex).Simplify()
	m, err := compile(regex)
	return Matcher{m}, err
}

func compile(r *syntax.Regexp) (stringMatcher, error) {
	switch {
	case r.Op == syntax.OpLiteral:
		s := string(r.Rune)
		return &substringMatcher{s, []byte(s)}, nil

	case isExactLiteral(r):
		s := string(r.Sub[1].Rune)
		return &equalsMatcher{s, []byte(s)}, nil

	case isAltLiterals(r):
		var literals [][]byte
		for _, sub := range r.Sub {
			literals = append(literals, []byte(string(sub.Rune)))
		}
		return &altSubstringMatcher{literals}, nil

	case isOneOfLiterals(r):
		var literals [][]byte
		for _, sub := range r.Sub[1].Sub {
			literals = append(literals, []byte(string(sub.Rune)))
		}
		return &oneOfMatcher{literals}, nil

	case isPrefixLiteral(r):
		s := []byte(string(r.Sub[1].Rune))
		return &prefixMatcher{s}, nil

	case isPrefixAltLiterals(r):
		var literals [][]byte
		for _, sub := range r.Sub[1].Sub {
			literals = append(literals, []byte(string(sub.Rune)))
		}
		return &altPrefixMatcher{literals}, nil

	case isPrefixNumDate(r):
		return compilePrefixNumDate(r)

	case isEmptyText(r):
		var m *emptyStringMatcher
		return m, nil

	case isEmptyTextWithWhitespace(r):
		var m *emptyWhiteStringMatcher
		return m, nil

	case isAnyMatch(r):
		var m *matchAny
		return m, nil

	default:

		r, err := regexp.Compile(r.String())
		if err != nil {
			return nil, err
		}
		return r, nil
	}
}

// isPrefixLiteral checks regular expression being literal checking string
// starting with literal pattern (like '^PATTERN')
func isPrefixLiteral(r *syntax.Regexp) bool {
	return r.Op == syntax.OpConcat &&
		len(r.Sub) == 2 &&
		r.Sub[0].Op == syntax.OpBeginText &&
		r.Sub[1].Op == syntax.OpLiteral
}

func isAltLiterals(r *syntax.Regexp) bool {
	if r.Op != syntax.OpAlternate {
		return false
	}

	for _, sub := range r.Sub {
		if sub.Op != syntax.OpLiteral {
			return false
		}
	}
	return true
}

func isExactLiteral(r *syntax.Regexp) bool {
	return r.Op == syntax.OpConcat &&
		len(r.Sub) == 3 &&
		r.Sub[0].Op == syntax.OpBeginText &&
		r.Sub[1].Op == syntax.OpLiteral &&
		r.Sub[2].Op == syntax.OpEndText
}

func isOneOfLiterals(r *syntax.Regexp) bool {
	return r.Op == syntax.OpConcat &&
		len(r.Sub) == 3 &&
		r.Sub[0].Op == syntax.OpBeginText &&
		isAltLiterals(r.Sub[1]) &&
		r.Sub[2].Op == syntax.OpEndText
}

// isPrefixAltLiterals checks regular expression being alternative literals
// starting with literal pattern (like '^PATTERN')
func isPrefixAltLiterals(r *syntax.Regexp) bool {
	isPrefixAlt := r.Op == syntax.OpConcat &&
		len(r.Sub) == 2 &&
		r.Sub[0].Op == syntax.OpBeginText &&
		r.Sub[1].Op == syntax.OpAlternate
	if !isPrefixAlt {
		return false
	}

	for _, sub := range r.Sub[1].Sub {
		if sub.Op != syntax.OpLiteral {
			return false
		}
	}
	return true
}

func isPrefixNumDate(r *syntax.Regexp) bool {
	if r.Op != syntax.OpConcat || r.Sub[0].Op != syntax.OpBeginText {
		return false
	}

	i := 1
	if r.Sub[i].Op == syntax.OpLiteral {
		i++
	}

	// check starts with digits `\d{n}` or `[0-9]{n}`
	if !isMultiDigits(r.Sub[i]) {
		return false
	}
	i++

	for i < len(r.Sub) {
		// check separator
		if r.Sub[i].Op != syntax.OpLiteral {
			return false
		}
		i++

		// regex has 'OpLiteral' suffix, without any more digits/patterns following
		if i == len(r.Sub) {
			return true
		}

		// check digits
		if !isMultiDigits(r.Sub[i]) {
			return false
		}
		i++
	}

	return true
}

func isEmptyText(r *syntax.Regexp) bool {
	return eqRegex(r, patEmptyText)
}

func isEmptyTextWithWhitespace(r *syntax.Regexp) bool {
	return eqRegex(r, patEmptyWhiteText)
}

func isAnyMatch(r *syntax.Regexp) bool {
	return eqRegex(r, patAny1) ||
		eqRegex(r, patAny2) ||
		eqRegex(r, patAny3) ||
		eqRegex(r, patAny4)
}

func isDigitMatch(r *syntax.Regexp) bool {
	return eqRegex(r, patDigits)
}

func isMultiDigits(r *syntax.Regexp) bool {
	return isConcatRepetition(r) && isDigitMatch(r.Sub[0])
}

func isConcatRepetition(r *syntax.Regexp) bool {
	if r.Op != syntax.OpConcat {
		return false
	}

	first := r.Sub[0]
	for _, other := range r.Sub {
		if other != first { // concat repetitions reuse references => compare pointers
			return false
		}
	}

	return true
}

func eqRegex(r, proto *syntax.Regexp) bool {
	unmatchable := r.Op != proto.Op || r.Flags != proto.Flags ||
		(r.Min != proto.Min) || (r.Max != proto.Max) ||
		(len(r.Sub) != len(proto.Sub)) ||
		(len(r.Rune) != len(proto.Rune))

	if unmatchable {
		return false
	}

	for i := range r.Sub {
		if !eqRegex(r.Sub[i], proto.Sub[i]) {
			return false
		}
	}

	for i := range r.Rune {
		if r.Rune[i] != proto.Rune[i] {
			return false
		}
	}
	return true
}

func eqPrefixAnyRegex(r *syntax.Regexp, rs ...*syntax.Regexp) bool {
	for _, t := range rs {
		if eqPrefixRegex(r, t) {
			return true
		}
	}
	return false
}

func eqPrefixRegex(r, proto *syntax.Regexp) bool {
	if r.Op != syntax.OpConcat {
		return false
	}

	if proto.Op != syntax.OpConcat {
		if len(r.Sub) == 0 {
			return false
		}
		return eqRegex(r.Sub[0], proto)
	}

	if len(r.Sub) < len(proto.Sub) {
		return false
	}

	for i := range proto.Sub {
		if !eqRegex(r.Sub[i], proto.Sub[i]) {
			return false
		}
	}
	return true
}

func eqSuffixAnyRegex(r *syntax.Regexp, rs ...*syntax.Regexp) bool {
	for _, t := range rs {
		if eqSuffixRegex(r, t) {
			return true
		}
	}
	return false
}

func eqSuffixRegex(r, proto *syntax.Regexp) bool {
	if r.Op != syntax.OpConcat {
		return false
	}

	if proto.Op != syntax.OpConcat {
		i := len(r.Sub) - 1
		if i < 0 {
			return false
		}
		return eqRegex(r.Sub[i], proto)
	}

	if len(r.Sub) < len(proto.Sub) {
		return false
	}

	d := len(r.Sub) - len(proto.Sub)
	for i := range proto.Sub {
		if !eqRegex(r.Sub[d+i], proto.Sub[i]) {
			return false
		}
	}
	return true
}

func compilePrefixNumDate(r *syntax.Regexp) (stringMatcher, error) {
	m := &prefixNumDate{}

	i := 1
	if r.Sub[i].Op == syntax.OpLiteral {
		m.prefix = []byte(string(r.Sub[i].Rune))
		i++
	}

	digitLen := func(r *syntax.Regexp) int {
		if r.Op == syntax.OpConcat {
			return len(r.Sub)
		}
		return 1
	}

	var digits []int
	var seps [][]byte

	digits = append(digits, digitLen(r.Sub[i]))
	i++

	for i < len(r.Sub) {
		lit := []byte(string(r.Sub[i].Rune))
		i++

		// capture literal suffix
		if i == len(r.Sub) {
			m.suffix = lit
			break
		}

		seps = append(seps, lit)
		digits = append(digits, digitLen(r.Sub[i]))
		i++
	}

	minLen := len(m.prefix) + len(m.suffix)
	for _, d := range digits {
		minLen += d
	}
	for _, sep := range seps {
		minLen += len(sep)
	}

	m.digits = digits
	m.seps = seps
	m.minLen = minLen

	return m, nil
}

func (m *equalsMatcher) MatchString(s string) bool {
	return m.s == s
}

func (m *equalsMatcher) Match(bs []byte) bool {
	return bytes.Equal(bs, m.bs)
}

func (m *equalsMatcher) String() string {
	return fmt.Sprintf("<string '%v'>", m.s)
}

func (m *substringMatcher) MatchString(s string) bool {
	return strings.Contains(s, m.s)
}

func (m *substringMatcher) Match(bs []byte) bool {
	return bytes.Contains(bs, m.bs)
}

func (m *substringMatcher) String() string {
	return fmt.Sprintf("<substring '%v'>", m.s)
}

func (m *altSubstringMatcher) MatchString(s string) bool {
	return m.Match(StringToByteUnsafe(s))
}

func (m *altSubstringMatcher) Match(in []byte) bool {
	for _, literal := range m.literals {
		if bytes.Contains(in, literal) {
			return true
		}
	}
	return false
}

func (m *altSubstringMatcher) String() string {
	return fmt.Sprintf("<alt substring '%s'>", bytes.Join(m.literals, []byte(",")))
}

func (m *oneOfMatcher) MatchString(s string) bool {
	return m.Match(StringToByteUnsafe(s))
}

func (m *oneOfMatcher) Match(in []byte) bool {
	for _, literal := range m.literals {
		if bytes.Equal(in, literal) {
			return true
		}
	}
	return false
}

func (m *oneOfMatcher) String() string {
	return fmt.Sprintf("<one of '%s'>", bytes.Join(m.literals, []byte(",")))
}

func (m *prefixMatcher) MatchString(s string) bool {
	return len(s) >= len(m.s) && s[0:len(m.s)] == string(m.s)
}

func (m *prefixMatcher) Match(bs []byte) bool {
	return len(bs) >= len(m.s) && bytes.Equal(bs[0:len(m.s)], m.s)
}

func (m *prefixMatcher) String() string {
	return fmt.Sprintf("<prefix string '%v'>", string(m.s))
}

func (m *altPrefixMatcher) MatchString(in string) bool {
	for _, s := range m.literals {
		if len(in) >= len(s) && in[0:len(s)] == string(s) {
			return true
		}
	}
	return false
}

func (m *altPrefixMatcher) Match(bs []byte) bool {
	for _, s := range m.literals {
		if len(bs) >= len(s) && bytes.Equal(bs[0:len(s)], s) {
			return true
		}
	}
	return false
}

func (m *altPrefixMatcher) String() string {
	return fmt.Sprintf("<alt prefix string '%s'>", bytes.Join(m.literals, []byte(",")))
}

func (m *prefixNumDate) MatchString(in string) bool {
	return m.Match(StringToByteUnsafe(in))
}

func (m *prefixNumDate) Match(in []byte) bool {
	if len(in) < m.minLen {
		return false
	}

	pos := 0
	if m.prefix != nil {
		end := len(m.prefix)
		if !bytes.Equal(in[0:end], m.prefix) {
			return false
		}

		pos += end
	}

	for cnt := m.digits[0]; cnt > 0; cnt-- {
		v := in[pos]
		pos++
		if !('0' <= v && v <= '9') {
			return false
		}
	}

	for i := 1; i < len(m.digits); i++ {
		sep := m.seps[i-1]
		if !bytes.Equal(in[pos:pos+len(sep)], sep) {
			return false
		}

		pos += len(sep)
		for cnt := m.digits[i]; cnt > 0; cnt-- {
			v := in[pos]
			pos++
			if !('0' <= v && v <= '9') {
				return false
			}
		}
	}

	if sfx := m.suffix; len(sfx) > 0 {
		if !bytes.HasPrefix(in[pos:], sfx) {
			return false
		}
	}

	return true
}

func (m *prefixNumDate) String() string {
	return "<prefix num date>"
}

func (m *emptyStringMatcher) MatchString(s string) bool {
	return len(s) == 0
}

func (m *emptyStringMatcher) Match(bs []byte) bool {
	return len(bs) == 0
}

func (m *emptyStringMatcher) String() string {
	return "<empty>"
}

func (m *emptyWhiteStringMatcher) MatchString(s string) bool {
	for _, r := range s {
		if !(r == 0xa || r == 0xc || r == 0xd || r == 0x20 || r == '\t') {
			return false
		}
	}
	return true
}

func (m *emptyWhiteStringMatcher) Match(bs []byte) bool {
	for _, r := range ByteToStringUnsafe(bs) {
		if !(r == 0xa || r == 0xc || r == 0xd || r == 0x20 || r == '\t') {
			return false
		}
	}
	return true
}

func (m *emptyWhiteStringMatcher) String() string {
	return "<empty whitespace>"
}

func (m *matchAny) Match(_ []byte) bool       { return true }
func (m *matchAny) MatchString(_ string) bool { return true }
func (m *matchAny) String() string            { return "<any>" }

// optimize runs minimal regular expression optimizations
// until fix-point.
func optimize(r *syntax.Regexp) *syntax.Regexp {
	for {
		changed := false
		for _, t := range transformations {
			var upd bool
			upd, r = t(r)
			changed = changed || upd
		}

		if !changed {
			return r
		}
	}
}

// Simplify regular expression by stdlib.
func simplify(r *syntax.Regexp) (bool, *syntax.Regexp) {
	return false, r.Simplify()
}

// uncapture optimizes regular expression by removing capture groups from
// regular expression potentially allocating memory when executed.
func uncapture(r *syntax.Regexp) (bool, *syntax.Regexp) {
	if r.Op == syntax.OpCapture {
		// try to uncapture
		if len(r.Sub) == 1 {
			_, sub := uncapture(r.Sub[0])
			return true, sub
		}

		tmp := *r
		tmp.Op = syntax.OpConcat
		r = &tmp
	}

	sub := make([]*syntax.Regexp, len(r.Sub))
	modified := false
	for i := range r.Sub {
		var m bool
		m, sub[i] = uncapture(r.Sub[i])
		modified = modified || m
	}

	if !modified {
		return false, r
	}

	tmp := *r
	tmp.Sub = sub
	return true, &tmp
}

// trimLeft removes not required '.*' from beginning of regular expressions.
func trimLeft(r *syntax.Regexp) (bool, *syntax.Regexp) {
	if eqPrefixAnyRegex(r, patDotStar, patNullBeginDotStar) {
		tmp := *r
		tmp.Sub = tmp.Sub[1:]
		return true, &tmp
	}

	return false, r
}

// trimRight removes not required '.*' from end of regular expressions.
func trimRight(r *syntax.Regexp) (bool, *syntax.Regexp) {
	if eqSuffixAnyRegex(r, patDotStar, patNullEndDotStar) {
		i := len(r.Sub) - 1
		tmp := *r
		tmp.Sub = tmp.Sub[0:i]
		return true, &tmp
	}

	return false, r
}

// unconcat removes intermediate regular expression concatenations generated by
// parser if concatenation contains only 1 element. Removal of object from
// parse-tree can enable other optimization to fire.
func unconcat(r *syntax.Regexp) (bool, *syntax.Regexp) {
	switch {
	case r.Op == syntax.OpConcat && len(r.Sub) <= 1:
		if len(r.Sub) == 1 {
			return true, r.Sub[0]
		}

		return true, &syntax.Regexp{
			Op:    syntax.OpEmptyMatch,
			Flags: r.Flags,
		}

	case r.Op == syntax.OpRepeat && r.Min == r.Max && r.Min == 1:
		return true, r.Sub[0]
	}

	return false, r
}

// concatRepetition concatenates 2 consecutive repeated sub-patterns into a
// repetition of length 2.
func concatRepetition(r *syntax.Regexp) (bool, *syntax.Regexp) {
	if r.Op != syntax.OpConcat {
		// don't iterate sub-expressions if top-level is no OpConcat
		return false, r
	}

	// check if concatenated op is already a repetition
	if isConcatRepetition(r) {
		return false, r
	}

	// concatenate repetitions in sub-expressions first
	var subs []*syntax.Regexp
	changed := false
	for _, sub := range r.Sub {
		changedSub, tmp := concatRepetition(sub)
		changed = changed || changedSub
		subs = append(subs, tmp)
	}

	var concat []*syntax.Regexp
	lastMerged := -1
	for i, j := 0, 1; j < len(subs); i, j = j, j+1 {
		if subs[i].Op == syntax.OpRepeat && eqRegex(subs[i].Sub[0], subs[j]) {
			r := subs[i]
			concat = append(concat,
				&syntax.Regexp{
					Op:    syntax.OpRepeat,
					Sub:   r.Sub,
					Min:   r.Min + 1,
					Max:   r.Max + 1,
					Flags: r.Flags,
				},
			)

			lastMerged = j
			changed = true
			j++
			continue
		}

		if isConcatRepetition(subs[i]) && eqRegex(subs[i].Sub[0], subs[j]) {
			r := subs[i]
			concat = append(concat,
				&syntax.Regexp{
					Op:    syntax.OpConcat,
					Sub:   append(r.Sub, r.Sub[0]),
					Flags: r.Flags,
				},
			)

			lastMerged = j
			changed = true
			j++
			continue
		}

		if eqRegex(subs[i], subs[j]) {
			r := subs[i]
			concat = append(concat,
				&syntax.Regexp{
					Op:    syntax.OpRepeat,
					Sub:   []*syntax.Regexp{r},
					Min:   2,
					Max:   2,
					Flags: r.Flags,
				},
			)

			lastMerged = j
			changed = true
			j++
			continue
		}

		concat = append(concat, subs[i])
	}

	if lastMerged+1 != len(subs) {
		concat = append(concat, subs[len(subs)-1])
	}

	r = &syntax.Regexp{
		Op:    syntax.OpConcat,
		Sub:   concat,
		Flags: r.Flags,
	}
	return changed, r
}

// flattenRepetition flattens nested repetitions
func flattenRepetition(r *syntax.Regexp) (bool, *syntax.Regexp) {
	if r.Op != syntax.OpConcat {
		// don't iterate sub-expressions if top-level is no OpConcat
		return false, r
	}

	sub := r.Sub
	inRepetition := false
	if isConcatRepetition(r) {
		sub = sub[:1]
		inRepetition = true

		// create flattened regex repetition multiplying count
		// if nexted expression is also a repetition
		if s := sub[0]; isConcatRepetition(s) {
			count := len(s.Sub) * len(r.Sub)
			return true, &syntax.Regexp{
				Op:    syntax.OpRepeat,
				Sub:   s.Sub[:1],
				Min:   count,
				Max:   count,
				Flags: r.Flags | s.Flags,
			}
		}
	}

	// recursively check if we can flatten sub-expressions
	changed := false
	for i, s := range sub {
		upd, tmp := flattenRepetition(s)
		changed = changed || upd
		sub[i] = tmp
	}

	if !changed {
		return false, r
	}

	// fix up top-level repetition with modified one
	tmp := *r
	if inRepetition {
		for i := range r.Sub {
			tmp.Sub[i] = sub[0]
		}
	} else {
		tmp.Sub = sub
	}
	return changed, &tmp
}
