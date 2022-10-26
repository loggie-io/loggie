package condition

import "testing"

func TestEqual(t *testing.T) {
	tests := []struct {
		name   string
		val1   interface{}
		val2   interface{}
		wanted bool
	}{
		{
			name:   "string num eq",
			val1:   "1.0",
			val2:   "1.0",
			wanted: true,
		},
		{
			name:   "num eq",
			val1:   1.022,
			val2:   1.022,
			wanted: true,
		},
		{
			name:   "string eq",
			val1:   "aaa",
			val2:   "aaa",
			wanted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag, err := Equal(tt.val1, tt.val2)
			if err != nil {
				t.Errorf("err should be nil")
			}
			if !flag {
				t.Errorf("%s and %s should be equal", tt.val1, tt.val2)
			}
		})
	}
}
