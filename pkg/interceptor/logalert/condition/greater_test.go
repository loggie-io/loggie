package condition

import "testing"

func TestGreater(t *testing.T) {
	tests := []struct {
		name   string
		val1   interface{}
		val2   interface{}
		wanted bool
	}{
		{
			name:   "string eq",
			val1:   "1.2",
			val2:   "1.0",
			wanted: true,
		},
		{
			name:   "num eq",
			val1:   1.222,
			val2:   1.022,
			wanted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag, err := Greater(tt.val1, tt.val2)
			if err != nil {
				t.Errorf("err should be nil")
			}
			if !flag {
				t.Errorf("%s should be greater than %s", tt.val1, tt.val2)
			}
		})
	}
}
