package util

import (
	"fmt"
	"reflect"
	"regexp"
	"testing"
)

func TestRegx(t *testing.T) {
	r := regexp.MustCompile("^(?P<Year>\\d{4})-(?P<Month>\\d{2})-(?P<Day>\\d{2})$")
	res := r.FindStringSubmatch(`2015-05-27`)
	l := len(res)
	fmt.Println(l)
	names := r.SubexpNames()
	for i := range res {
		if i != 0 {
			fmt.Println(names[i], res[i])
		}
	}
}

func TestMatchGroupWithRegex(t *testing.T) {
	type args struct {
		compRegEx *regexp.Regexp
		context   string
	}
	tests := []struct {
		name          string
		args          args
		wantParamsMap map[string]string
	}{
		{
			name: "test date",
			args: args{
				compRegEx: MustCompilePatternWithJavaStyle("^(?P<Year>\\d{4})-(?P<Month>\\d{2})-(?P<Day>\\d{2})$"),
				context:   "2021-11-01",
			},
			wantParamsMap: map[string]string{
				"Year":  "2021",
				"Month": "11",
				"Day":   "01",
			},
		},
		{
			name: "test log split",
			args: args{
				compRegEx: MustCompilePatternWithJavaStyle("^\\[(?<loglevel>[^,]*)\\]\\s*Lag\\s*monitor,\\s*time\\:(?<time>[^,]*),\\s*groupid\\:\\s*(?<groupid>[^,]*),\\s*current_lag_sum\\:(?<currentlag>[^,]*),\\s*threshold\\:(?<threshold>[^,]*),\\s*topics\\:(?<topic>[^,]*)"),
				context:   "[WARN]  Lag monitor, time:2019-10-31 15:31:05, groupid: fdata-dmvipappnewsaleratejob, current_lag_sum: 17, threshold: 60000, topics: FDATA-DWD_ORDER_USER",
			},
			wantParamsMap: map[string]string{
				"currentlag": " 17",
				"groupid":    "fdata-dmvipappnewsaleratejob",
				"loglevel":   "WARN",
				"threshold":  " 60000",
				"time":       "2019-10-31 15:31:05",
				"topic":      " FDATA-DWD_ORDER_USER",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotParamsMap := MatchGroupWithRegex(tt.args.compRegEx, tt.args.context); !reflect.DeepEqual(gotParamsMap, tt.wantParamsMap) {
				t.Errorf("MatchGroupWithRegex() = %v, want %v", gotParamsMap, tt.wantParamsMap)
			}
		})
	}
}
