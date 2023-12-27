package process

import (
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/filestream/ack"
	"github.com/loggie-io/loggie/pkg/source/filestream/process/define"
	"github.com/loggie-io/loggie/pkg/source/filestream/storage"
	"github.com/loggie-io/loggie/pkg/source/filestream/util"
	"time"
)

type TransporterProcessor struct {
	progress    storage.CollectProgress
	eventPool   *event.Pool
	next        define.CollectProcessor
	meta        map[string]interface{}
	ackEnable   bool
	productChan api.ProductFunc
	begin       uint64
}

func NewTransporterProcessor(progress storage.CollectProgress, pool *event.Pool, productChan api.ProductFunc, ackEnable bool) define.CollectProcessor {
	meta := make(map[string]interface{}, 3)
	meta["module"] = "loggie"
	meta["type"] = ""
	meta["version"] = global.GetVersion()

	return &TransporterProcessor{
		progress:    progress,
		meta:        meta,
		eventPool:   pool,
		ackEnable:   ackEnable,
		productChan: productChan,
		begin:       0,
	}
}

func (t *TransporterProcessor) Next(next define.CollectProcessor) {
	t.next = next
}

func (t *TransporterProcessor) OnError(error) {

}

// OnProcess 写入queue
func (t *TransporterProcessor) OnProcess(ctx define.Context) {
	if len(ctx.GetBuf()) == 0 {
		return
	}
	e := t.eventPool.Get()
	var collection map[string]interface{}
	collection = make(map[string]interface{}, 5)
	header := e.Header()
	collection["@collectiontime"] = time.Now()
	collection["@message"] = util.Bytes2Str(ctx.GetBuf())
	collection["@path"] = ctx.GetPath()
	collection["@rownumber"] = t.progress.GetCollectLine()
	collection["@seq"] = t.progress.GetCollectSeq()
	header["collection"] = collection
	header["metadata"] = t.meta

	e.Meta().Set(ack.AckMarkName, ctx.GetAckMark())
	e.Fill(e.Meta(), header, []byte{})
	t.productChan(e)
	t.next.OnProcess(ctx)
}

func (t *TransporterProcessor) OnComplete(ctx define.Context) {
	t.next.OnComplete(ctx)
	if t.ackEnable {
		return
	}
	err := t.progress.Save()
	if err != nil {
		log.Error("t.progress.Save error:%s", err)
		return
	}
	return
}
