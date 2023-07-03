/*
Copyright 2023 Loggie Authors

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

package elasticsearch

// BulkIndexerResponse represents the Elasticsearch response.
//
type BulkIndexerResponse struct {
	Took      int                                   `json:"took"`
	HasErrors bool                                  `json:"errors"`
	Items     []map[string]*BulkIndexerResponseItem `json:"items,omitempty"`
}

// BulkIndexerResponseItem represents the Elasticsearch response item.
//
type BulkIndexerResponseItem struct {
	Index      string `json:"_index"`
	DocumentID string `json:"_id"`
	Version    int64  `json:"_version"`
	Result     string `json:"result"`
	Status     int    `json:"status"`
	SeqNo      int64  `json:"_seq_no"`
	PrimTerm   int64  `json:"_primary_term"`

	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`

	Error struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Cause  struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"caused_by"`
	} `json:"error,omitempty"`
}

func (r *BulkIndexerResponse) Failed() []*BulkIndexerResponseItem {
	if r.Items == nil {
		return nil
	}
	var errors []*BulkIndexerResponseItem
	for _, item := range r.Items {
		for _, result := range item {
			if !(result.Status >= 200 && result.Status <= 299) {
				errors = append(errors, result)
			}
		}
	}
	return errors
}
