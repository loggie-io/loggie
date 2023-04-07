package condition

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExtractFunctions(t *testing.T) {
	tests := []struct {
		input             string
		expectedConnector string
		expectedFunctions []string
	}{
		{
			input:             "func1(a, ERROR)",
			expectedConnector: "",
			expectedFunctions: []string{"func1(a, ERROR)"},
		},
		{
			input:             "func1(a, ERROR) OR func2(a, foo) OR func3(b, bar)",
			expectedConnector: "OR",
			expectedFunctions: []string{"func1(a, ERROR)", "func2(a, foo)", "func3(b, bar)"},
		},
		{
			input:             "funcA(a, foo) AND funcB(b, bar)",
			expectedConnector: "AND",
			expectedFunctions: []string{"funcA(a, foo)", "funcB(b, bar)"},
		},
		{
			input:             "contain(a, foo) OR equal(b, bar)",
			expectedConnector: "OR",
			expectedFunctions: []string{"contain(a, foo)", "equal(b, bar)"},
		},
		{
			input:             "NOT contain(a, foo) OR equal(b, bar)",
			expectedConnector: "OR",
			expectedFunctions: []string{"NOT contain(a, foo)", "equal(b, bar)"},
		},
		{
			input:             "NOT contain(a, foo) AND NOT equal(b, bar)",
			expectedConnector: "AND",
			expectedFunctions: []string{"NOT contain(a, foo)", "NOT equal(b, bar)"},
		},
	}

	for _, test := range tests {
		connector, fn := extractFunctions(test.input)
		assert.Equal(t, test.expectedConnector, connector, "input: %q", test.input)
		assert.Equal(t, test.expectedFunctions, fn, "input: %q", test.input)
	}
}
