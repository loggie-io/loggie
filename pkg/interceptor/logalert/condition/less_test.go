package condition

import "testing"

func TestLess(t *testing.T) {
	tests := []struct {
		name   string
		val1   interface{}
		val2   interface{}
		wanted bool
	}{
		{
			name:   "string eq",
			val1:   "1.0",
			val2:   "1.2",
			wanted: true,
		},
		{
			name:   "num eq",
			val1:   1.022,
			val2:   1.222,
			wanted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag, err := Less(tt.val1, tt.val2)
			if err != nil {
				t.Errorf("err should be nil")
			}
			if !flag {
				t.Errorf("%s should be less than %s", tt.val1, tt.val2)
			}
		})
	}

}
