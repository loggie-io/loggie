package sls

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
)

const (
	DataSourceOSS        DataSourceType = "AliyunOSS"
	DataSourceBSS        DataSourceType = "AliyunBSS"
	DataSourceMaxCompute DataSourceType = "AliyunMaxCompute"
	DataSourceJDBC       DataSourceType = "JDBC"
	DataSourceKafka      DataSourceType = "Kafka"
	DataSourceCMS        DataSourceType = "AliyunCloudMonitor"
	DataSourceGeneral    DataSourceType = "General"

	OSSDataFormatTypeLine          OSSDataFormatType = "Line"
	OSSDataFormatTypeMultiline     OSSDataFormatType = "Multiline"
	OSSDataFormatTypeJSON          OSSDataFormatType = "JSON"
	OSSDataFormatTypeParquet       OSSDataFormatType = "Parquet"
	OSSDataFormatTypeDelimitedText OSSDataFormatType = "DelimitedText"

	KafkaValueTypeText KafkaValueType = "Text"
	KafkaValueTypeJSON KafkaValueType = "JSON"

	KafkaPositionGroupOffsets KafkaPosition = "GROUP_OFFSETS"
	KafkaPositionEarliest     KafkaPosition = "EARLIEST"
	KafkaPositionLatest       KafkaPosition = "LATEST"
	KafkaPositionTimeStamp    KafkaPosition = "TIMESTAMP"

	DataSinkLOG     DataSinkType = "AliyunLOG"
	DataSinkOSS     DataSinkType = "AliyunOSS"
	DataSinkADB     DataSinkType = "AliyunADB"
	DataSinkTSDB    DataSinkType = "AliyunTSDB"
	DataSinkODPS    DataSinkType = "AliyunODPS"
	DataSinkGENERAL DataSinkType = "General"

	OSSContentDetailTypeParquet OSSContentType = "parquet"
	OSSContentDetailTypeORC     OSSContentType = "orc"
	OSSContentDetailTypeCSV     OSSContentType = "csv"
	OSSContentDetailTypeJSON    OSSContentType = "json"

	OSSCompressionTypeNone   OSSCompressionType = "none"
	OSSCompressionTypeZstd   OSSCompressionType = "zstd"
	OSSCompressionTypeGzip   OSSCompressionType = "gzip"
	OSSCompressionTypeSnappy OSSCompressionType = "snappy"

	ExportVersion2 ExportVersion = "v2.0" // new versions of OSSExport, ODPSExport Version property must use this field, otherwise the service may be unavailable or incomplete
)

type (
	BaseJob struct {
		Name           string  `json:"name"`
		DisplayName    string  `json:"displayName,omitempty"`
		Description    string  `json:"description,omitempty"`
		Type           JobType `json:"type"`
		Recyclable     bool    `json:"recyclable"`
		CreateTime     int64   `json:"createTime"`
		LastModifyTime int64   `json:"lastModifyTime"`
	}

	ScheduledJob struct {
		BaseJob
		Status     string    `json:"status"`
		Schedule   *Schedule `json:"schedule"`
		ScheduleId string    `json:"scheduleId"`
	}

	Ingestion struct {
		ScheduledJob
		IngestionConfiguration *IngestionConfiguration `json:"configuration"`
	}

	IngestionConfiguration struct {
		Version          string      `json:"version"`
		LogStore         string      `json:"logstore"`
		NumberOfInstance int32       `json:"numberOfInstance"`
		DataSource       interface{} `json:"source"`
	}

	DataSourceType string

	DataSource struct {
		DataSourceType DataSourceType `json:"type"`
	}

	// >>> ingestion oss source
	OSSDataFormatType string

	AliyunOSSSource struct {
		DataSource
		Bucket                  string      `json:"bucket"`
		Endpoint                string      `json:"endpoint"`
		RoleArn                 string      `json:"roleARN"`
		Prefix                  string      `json:"prefix,omitempty"`
		Pattern                 string      `json:"pattern,omitempty"`
		CompressionCodec        string      `json:"compressionCodec,omitempty"`
		Encoding                string      `json:"encoding,omitempty"`
		Format                  interface{} `json:"format,omitempty"`
		RestoreObjectEnable     bool        `json:"restoreObjectEnable"`
		LastModifyTimeAsLogTime bool        `json:"lastModifyTimeAsLogTime"`
	}

	OSSDataFormat struct {
		Type       OSSDataFormatType `json:"type"`
		TimeFormat string            `json:"timeFormat"`
		TimeZone   string            `json:"timeZone"`
	}

	LineFormat struct {
		OSSDataFormat
		TimePattern string `json:"timePattern"`
	}

	MultiLineFormat struct {
		LineFormat
		MaxLines     int64  `json:"maxLines,omitempty"`
		Negate       bool   `json:"negate"`
		Match        string `json:"match"`
		Pattern      string `json:"pattern"`
		FlushPattern string `json:"flushPattern"`
	}

	StructureDataFormat struct {
		OSSDataFormat
		TimeField string `json:"timeField"`
	}

	JSONFormat struct {
		StructureDataFormat
		SkipInvalidRows bool `json:"skipInvalidRows"`
	}

	ParquetFormat struct {
		StructureDataFormat
	}

	DelimitedTextFormat struct {
		StructureDataFormat
		FieldNames       []string `json:"fieldNames"`
		FieldDelimiter   string   `json:"fieldDelimiter"`
		QuoteChar        string   `json:"quoteChar"`
		EscapeChar       string   `json:"escapeChar"`
		SkipLeadingRows  int64    `json:"skipLeadingRows"`
		MaxLines         int64    `json:"maxLines"`
		FirstRowAsHeader bool     `json:"firstRowAsHeader"`
	}

	// ingestion maxcompute source >>>
	AliyunMaxComputeSource struct {
		DataSource
		AccessKeyID     string `json:"accessKeyID"`
		AccessKeySecret string `json:"accessKeySecret"`
		Endpoint        string `json:"endpoint"`
		TunnelEndpoint  string `json:"tunnelEndpoint,omitempty"`
		Project         string `json:"project"`
		Table           string `json:"table"`
		PartitionSpec   string `json:"partitionSpec"`
		TimeField       string `json:"timeField"`
		TimeFormat      string `json:"timeFormat"`
		TimeZone        string `json:"timeZone"`
	}

	// ingestion cloud monitor source
	AliyunCloudMonitorSource struct {
		DataSource
		AccessKeyID     string   `json:"accessKeyID"`
		AccessKeySecret string   `json:"accessKeySecret"`
		StartTime       int64    `json:"startTime"`
		Namespaces      []string `json:"namespaces"`
		OutputType      string   `json:"outputType"`
		DelayTime       int64    `json:"delayTime"`
	}

	// ingestion kafka source
	KafkaValueType string
	KafkaPosition  string
	KafkaSource    struct {
		DataSource
		Topics           string            `json:"topics"`
		BootStrapServers string            `json:"bootstrapServers"`
		ValueType        KafkaValueType    `json:"valueType"`
		FromPosition     KafkaPosition     `json:"fromPosition"`
		FromTimeStamp    int64             `json:"fromTimestamp"`
		ToTimeStamp      int64             `json:"toTimestamp"`
		TimeField        string            `json:"timeField"`
		TimePattern      string            `json:"timePattern"`
		TimeFormat       string            `json:"timeFormat"`
		TimeZone         string            `json:"timeZone"`
		AdditionalProps  map[string]string `json:"additionalProps"`
	}

	// ingestion JDBC source
	AliyunBssSource struct {
		DataSource
		RoleArn      string `json:"roleARN"`
		HistoryMonth int64  `json:"historyMonth"`
	}

	// ingestion general source
	IngestionGeneralSource struct {
		DataSource
		Fields map[string]interface{}
	}

	Export struct {
		ScheduledJob
		ExportConfiguration *ExportConfiguration `json:"configuration"`
	}

	ExportVersion string

	ExportConfiguration struct {
		FromTime   int64             `json:"fromTime"`
		ToTime     int64             `json:"toTime"`
		LogStore   string            `json:"logstore"`
		Parameters map[string]string `json:"parameters"`
		RoleArn    string            `json:"roleArn"`
		Version    ExportVersion     `json:"version"`
		DataSink   DataSink          `json:"sink"`
	}

	DataSink interface {
		DataSinkType() DataSinkType
	}

	DataSinkType       string
	OSSContentType     string
	OSSCompressionType string

	AliyunOSSSink struct {
		Type            DataSinkType       `json:"type"`
		RoleArn         string             `json:"roleArn"`
		Bucket          string             `json:"bucket"`
		Prefix          string             `json:"prefix"`
		Suffix          string             `json:"suffix"`
		PathFormat      string             `json:"pathFormat"`
		PathFormatType  string             `json:"pathFormatType"`
		BufferSize      int64              `json:"bufferSize"`
		BufferInterval  int64              `json:"bufferInterval"`
		TimeZone        string             `json:"timeZone"`
		ContentType     OSSContentType     `json:"contentType"`
		CompressionType OSSCompressionType `json:"compressionType"`
		ContentDetail   string             `json:"contentDetail"` // according to ContentType, its value is the corresponding CsvContentDetail, JsonContentDetail, ParquetContentDetail, OrcContentDetail serialized string
	}

	CsvContentDetail struct {
		ColumnNames []string `json:"columns"`
		Delimiter   string   `json:"delimiter"`
		Quote       string   `json:"quote"`
		Escape      string   `json:"escape"`
		Null        string   `json:"null"`
		Header      bool     `json:"header"`
		LineFeed    string   `json:"lineFeed"`
	}

	JsonContentDetail struct {
		EnableTag bool `json:"enableTag"`
	}

	ParquetContentDetail ColumnStorageContentDetail
	OrcContentDetail     ColumnStorageContentDetail

	ColumnStorageContentDetail struct {
		Columns []Column `json:"columns"`
	}

	Column struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	AliyunODPSSink struct {
		Type                DataSinkType `json:"type"`
		OdpsRolearn         string       `json:"odpsRolearn"`
		OdpsEndpoint        string       `json:"odpsEndpoint"`
		OdpsTunnelEndpoint  string       `json:"odpsTunnelEndpoint"`
		OdpsProject         string       `json:"odpsProject"`
		OdpsTable           string       `json:"odpsTable"`
		TimeZone            string       `json:"timeZone"`
		PartitionTimeFormat string       `json:"partitionTimeFormat"`
		Fields              []string     `json:"fields"`
		PartitionColumn     []string     `json:"partitionColumn"`
		OdpsAccessKeyId     string       `json:"odpsAccessKeyId"`
		OdpsAccessSecret    string       `json:"odpsAccessAecret"`
	}

	AliyunGeneralSink struct {
		Type   DataSinkType           `json:"type"`
		Fields map[string]interface{} `json:"fields"`
	}
)

func (_ *AliyunOSSSink) DataSinkType() DataSinkType {
	return DataSinkOSS
}
func (_ *AliyunODPSSink) DataSinkType() DataSinkType {
	return DataSinkODPS
}
func (_ *AliyunGeneralSink) DataSinkType() DataSinkType {
	return DataSinkGENERAL
}

func (e *ExportConfiguration) UnmarshalJSON(data []byte) error {
	c := map[string]interface{}{}
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	sinkMap := c["sink"].(map[string]interface{})
	t := sinkMap["type"].(string)
	var sink DataSink
	sinkBytes, err := json.Marshal(sinkMap)
	if err != nil {
		return err
	} else if string(DataSinkOSS) == t {
		sink = &AliyunOSSSink{}
	} else if string(DataSinkODPS) == t {
		sink = &AliyunODPSSink{}
	} else if string(DataSinkGENERAL) == t {
		sink = &AliyunGeneralSink{}
	} else { // TODO: other sinks to be implemented
		return errors.New(fmt.Sprintf("unsupported sink type:%s", t))
	}
	if err = json.Unmarshal(sinkBytes, sink); err != nil {
		return err
	}
	parameters := map[string]string{}
	for k, v := range c["parameters"].(map[string]interface{}) {
		parameters[k] = v.(string)
	}
	*e = ExportConfiguration{
		FromTime:   int64(c["fromTime"].(float64)),
		ToTime:     int64(c["toTime"].(float64)),
		LogStore:   c["logstore"].(string),
		Parameters: parameters,
		RoleArn:    c["roleArn"].(string),
		Version:    ExportVersion(c["version"].(string)),
		DataSink:   sink,
	}
	return nil
}

func (c *Client) CreateIngestion(project string, ingestion *Ingestion) error {
	body, err := json.Marshal(ingestion)
	if err != nil {
		return NewClientError(err)
	}
	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}
	uri := "/jobs"
	r, err := c.request(project, "POST", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) UpdateIngestion(project string, ingestion *Ingestion) error {
	body, err := json.Marshal(ingestion)
	if err != nil {
		return NewClientError(err)
	}
	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + ingestion.Name
	r, err := c.request(project, "PUT", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) GetIngestion(project string, name string) (*Ingestion, error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + name
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	buf, _ := ioutil.ReadAll(r.Body)
	ingestion := &Ingestion{}
	if err = json.Unmarshal(buf, ingestion); err != nil {
		err = NewClientError(err)
	}
	return ingestion, err
}

func (c *Client) ListIngestion(project, logstore, name, displayName string, offset, size int) (ingestions []*Ingestion, total, count int, error error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	v := url.Values{}
	v.Add("logstore", logstore)
	v.Add("jobName", name)
	if displayName != "" {
		v.Add("displayName", displayName)
	}
	v.Add("jobType", "Ingestion")
	v.Add("offset", fmt.Sprintf("%d", offset))
	v.Add("size", fmt.Sprintf("%d", size))
	uri := "/jobs?" + v.Encode()
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	defer r.Body.Close()
	type ingestionList struct {
		Total   int          `json:"total"`
		Count   int          `json:"count"`
		Results []*Ingestion `json:"results"`
	}
	buf, _ := ioutil.ReadAll(r.Body)
	is := &ingestionList{}
	if err = json.Unmarshal(buf, is); err != nil {
		err = NewClientError(err)
	}
	return is.Results, is.Total, is.Count, err
}

func (c *Client) DeleteIngestion(project string, name string) error {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + name
	r, err := c.request(project, "DELETE", uri, h, nil)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}

func (c *Client) CreateExport(project string, export *Export) error {
	body, err := json.Marshal(export)
	if err != nil {
		return NewClientError(err)
	}
	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}
	uri := "/jobs"
	r, err := c.request(project, "POST", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}
func (c *Client) UpdateExport(project string, export *Export) error {
	body, err := json.Marshal(export)
	if err != nil {
		return NewClientError(err)
	}
	h := map[string]string{
		"x-log-bodyrawsize": fmt.Sprintf("%v", len(body)),
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + export.Name
	r, err := c.request(project, "PUT", uri, h, body)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}
func (c *Client) GetExport(project, name string) (*Export, error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + name
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	buf, _ := ioutil.ReadAll(r.Body)
	export := &Export{}
	if err = json.Unmarshal(buf, export); err != nil {
		err = NewClientError(err)
	}
	return export, err
}
func (c *Client) ListExport(project, logstore, name, displayName string, offset, size int) (exports []*Export, total, count int, error error) {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	v := url.Values{}
	v.Add("logstore", logstore)
	v.Add("jobName", name)
	if displayName != "" {
		v.Add("displayName", displayName)
	}
	v.Add("jobType", "Export")
	v.Add("offset", fmt.Sprintf("%d", offset))
	v.Add("size", fmt.Sprintf("%d", size))
	uri := "/jobs?" + v.Encode()
	r, err := c.request(project, "GET", uri, h, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	defer r.Body.Close()
	type exportList struct {
		Total   int       `json:"total"`
		Count   int       `json:"count"`
		Results []*Export `json:"results"`
	}
	buf, _ := ioutil.ReadAll(r.Body)
	el := &exportList{}
	if err = json.Unmarshal(buf, el); err != nil {
		err = NewClientError(err)
	}
	return el.Results, el.Total, el.Count, err
}
func (c *Client) DeleteExport(project string, name string) error {
	h := map[string]string{
		"x-log-bodyrawsize": "0",
		"Content-Type":      "application/json",
	}
	uri := "/jobs/" + name
	r, err := c.request(project, "DELETE", uri, h, nil)
	if err != nil {
		return err
	}
	r.Body.Close()
	return nil
}
