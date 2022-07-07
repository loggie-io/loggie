package helper

import "testing"

func TestLabelsSubset(t *testing.T) {
	type args struct {
		i map[string]string
		j map[string]string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "is subset",
			args: args{
				i: map[string]string{
					"a": "b",
				},
				j: map[string]string{
					"a": "b",
					"c": "d",
				},
			},
			want: true,
		},
		{
			name: "is not subset",
			args: args{
				i: map[string]string{
					"a": "b",
					"c": "d",
				},
				j: map[string]string{
					"a": "b",
				},
			},
			want: false,
		},
		{
			name: "include wildcard *",
			args: args{
				i: map[string]string{
					"a": MatchAllToken,
				},
				j: map[string]string{
					"a": "b",
					"c": "d",
				},
			},
			want: true,
		},
		{
			name: "wildcard * not working",
			args: args{
				i: map[string]string{
					"a": MatchAllToken,
				},
				j: map[string]string{
					"c":   "d",
					"foo": "bar",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LabelsSubset(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("LabelsSubset() = %v, want %v", got, tt.want)
			}
		})
	}
}
