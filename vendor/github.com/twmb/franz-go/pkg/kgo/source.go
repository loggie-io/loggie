package kgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type readerFrom interface {
	ReadFrom([]byte) error
}

// A source consumes from an individual broker.
//
// As long as there is at least one active cursor, a source aims to have *one*
// buffered fetch at all times. As soon as the fetch is taken, a source issues
// another fetch in the background.
type source struct {
	cl     *Client // our owning client, for cfg, metadata triggering, context, etc.
	nodeID int32   // the node ID of the broker this sink belongs to

	// Tracks how many _failed_ fetch requests we have in a row (unable to
	// receive a response). Any response, even responses with an ErrorCode
	// set, are successful. This field is used for backoff purposes.
	consecutiveFailures int

	fetchState workLoop
	sem        chan struct{} // closed when fetchable, recreated when a buffered fetch exists
	buffered   bufferedFetch // contains a fetch the source has buffered for polling

	session fetchSession // supports fetch sessions as per KIP-227

	cursorsMu    sync.Mutex
	cursors      []*cursor // contains all partitions being consumed on this source
	cursorsStart int       // incremented every fetch req to ensure all partitions are fetched
}

func (cl *Client) newSource(nodeID int32) *source {
	s := &source{
		cl:     cl,
		nodeID: nodeID,
		sem:    make(chan struct{}),
	}
	if cl.cfg.disableFetchSessions {
		s.session.kill()
	}
	close(s.sem)
	return s
}

func (s *source) addCursor(add *cursor) {
	s.cursorsMu.Lock()
	add.cursorsIdx = len(s.cursors)
	s.cursors = append(s.cursors, add)
	s.cursorsMu.Unlock()

	// Adding a new cursor may allow a new partition to be fetched.
	// We do not need to cancel any current fetch nor kill the session,
	// since adding a cursor is non-destructive to work in progress.
	// If the session is currently stopped, this is a no-op.
	s.maybeConsume()
}

// Removes a cursor from the source.
//
// The caller should do this with a stopped session if necessary, which
// should clear any buffered fetch and reset the source's session.
func (s *source) removeCursor(rm *cursor) {
	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()

	if rm.cursorsIdx != len(s.cursors)-1 {
		s.cursors[rm.cursorsIdx], s.cursors[len(s.cursors)-1] = s.cursors[len(s.cursors)-1], nil
		s.cursors[rm.cursorsIdx].cursorsIdx = rm.cursorsIdx
	} else {
		s.cursors[rm.cursorsIdx] = nil // do not let the memory hang around
	}

	s.cursors = s.cursors[:len(s.cursors)-1]
	if s.cursorsStart == len(s.cursors) {
		s.cursorsStart = 0
	}
}

// cursor is where we are consuming from for an individual partition.
type cursor struct {
	topic     string
	topicID   [16]byte
	partition int32

	keepControl bool // whether to keep control records

	cursorsIdx int // updated under source mutex

	// The source we are currently on. This is modified in two scenarios:
	//
	//  * by metadata when the consumer session is completely stopped
	//
	//  * by a fetch when handling a fetch response that returned preferred
	//  replicas
	//
	// This is additionally read within a session when cursor is
	// transitioning from used to usable.
	source *source

	// useState is an atomic that has two states: unusable and usable.  A
	// cursor can be used in a fetch request if it is in the usable state.
	// Once used, the cursor is unusable, and will be set back to usable
	// one the request lifecycle is complete (a usable fetch response, or
	// once listing offsets or loading epochs completes).
	//
	// A cursor can be set back to unusable when sources are stopped. This
	// can be done if a group loses a partition, for example.
	//
	// The used state is exclusively updated by either building a fetch
	// request or when the source is stopped.
	useState uint32

	topicPartitionData // updated in metadata when session is stopped

	// cursorOffset is our epoch/offset that we are consuming. When a fetch
	// request is issued, we "freeze" a view of the offset and of the
	// leader epoch (see cursorOffsetNext for why the leader epoch). When a
	// buffered fetch is taken, we update the cursor.
	cursorOffset
}

// cursorOffset tracks offsets/epochs for a cursor.
type cursorOffset struct {
	// What the cursor is at: we request this offset next.
	offset int64

	// The epoch of the last record we consumed. Also used for KIP-320, if
	// we are fenced or we have an offset out of range error, we go into
	// the OffsetForLeaderEpoch recovery. The last consumed epoch tells the
	// broker which offset we want: either (a) the next offset if the last
	// consumed epoch is the current epoch, or (b) the offset of the first
	// record in the next epoch. This allows for exact offset resetting and
	// data loss detection.
	//
	// See kmsg.OffsetForLeaderEpochResponseTopicPartition for more
	// details.
	lastConsumedEpoch int32

	// The current high watermark of the partition. Uninitialized (0) means
	// we do not know the HWM, or there is no lag.
	hwm int64
}

// use, for fetch requests, freezes a view of the cursorOffset.
func (c *cursor) use() *cursorOffsetNext {
	// A source using a cursor has exclusive access to the use field by
	// virtue of that source building a request during a live session,
	// or by virtue of the session being stopped.
	c.useState = 0
	return &cursorOffsetNext{
		cursorOffset:       c.cursorOffset,
		from:               c,
		currentLeaderEpoch: c.leaderEpoch,
	}
}

// unset transitions a cursor to an unusable state when the cursor is no longer
// to be consumed. This is called exclusively after sources are stopped.
// This also unsets the cursor offset, which is assumed to be unused now.
func (c *cursor) unset() {
	c.useState = 0
	c.setOffset(cursorOffset{
		offset:            -1,
		lastConsumedEpoch: -1,
		hwm:               0,
	})
}

// usable returns whether a cursor can be used for building a fetch request.
func (c *cursor) usable() bool {
	return atomic.LoadUint32(&c.useState) == 1
}

// allowUsable allows a cursor to be fetched, and is called either in assigning
// offsets, or when a buffered fetch is taken or discarded,  or when listing /
// epoch loading finishes.
func (c *cursor) allowUsable() {
	atomic.SwapUint32(&c.useState, 1)
	c.source.maybeConsume()
}

// setOffset sets the cursors offset which will be used the next time a fetch
// request is built. This function is called under the source mutex while the
// source is stopped, and the caller is responsible for calling maybeConsume
// after.
func (c *cursor) setOffset(o cursorOffset) {
	c.cursorOffset = o
}

// cursorOffsetNext is updated while processing a fetch response.
//
// When a buffered fetch is taken, we update a cursor with the final values in
// the modified cursor offset.
type cursorOffsetNext struct {
	cursorOffset
	from *cursor

	// The leader epoch at the time we took this cursor offset snapshot. We
	// need to copy this rather than accessing it through `from` because a
	// fetch request can be canceled while it is being written (and reading
	// the epoch).
	//
	// The leader field itself is only read within the context of a session
	// while the session is alive, thus it needs no such guard.
	//
	// Basically, any field read in AppendTo needs to be copied into
	// cursorOffsetNext.
	currentLeaderEpoch int32
}

type cursorOffsetPreferred struct {
	cursorOffsetNext
	preferredReplica int32
}

// Moves a cursor from one source to another. This is done while handling
// a fetch response, which means within the context of a live session.
func (p *cursorOffsetPreferred) move() {
	c := p.from
	defer c.allowUsable()

	// Before we migrate the cursor, we check if the destination source
	// exists. If not, we do not migrate and instead force a metadata.

	c.source.cl.sinksAndSourcesMu.Lock()
	sns, exists := c.source.cl.sinksAndSources[p.preferredReplica]
	c.source.cl.sinksAndSourcesMu.Unlock()

	if !exists {
		c.source.cl.triggerUpdateMetadataNow("cursor moving to a different broker that is not yet known")
		return
	}

	// This remove clears the source's session and buffered fetch, although
	// we will not have a buffered fetch since moving replicas is called
	// before buffering a fetch.
	c.source.removeCursor(c)
	c.source = sns.source
	c.source.addCursor(c)
}

type cursorPreferreds []cursorOffsetPreferred

func (cs cursorPreferreds) eachPreferred(fn func(cursorOffsetPreferred)) {
	for _, c := range cs {
		fn(c)
	}
}

type usedOffsets map[string]map[int32]*cursorOffsetNext

func (os usedOffsets) eachOffset(fn func(*cursorOffsetNext)) {
	for _, ps := range os {
		for _, o := range ps {
			fn(o)
		}
	}
}

func (os usedOffsets) finishUsingAllWithSet() {
	os.eachOffset(func(o *cursorOffsetNext) { o.from.setOffset(o.cursorOffset); o.from.allowUsable() })
}

func (os usedOffsets) finishUsingAll() {
	os.eachOffset(func(o *cursorOffsetNext) { o.from.allowUsable() })
}

// bufferedFetch is a fetch response waiting to be consumed by the client.
type bufferedFetch struct {
	fetch Fetch

	doneFetch   chan<- struct{} // when unbuffered, we send down this
	usedOffsets usedOffsets     // what the offsets will be next if this fetch is used
}

func (s *source) hook(f *Fetch, buffered, polled bool) {
	s.cl.cfg.hooks.each(func(h Hook) {
		switch h := h.(type) {
		case HookFetchRecordBuffered:
			if !buffered {
				return
			}
			for i := range f.Topics {
				t := &f.Topics[i]
				for j := range t.Partitions {
					p := &t.Partitions[j]
					for _, r := range p.Records {
						h.OnFetchRecordBuffered(r)
					}
				}
			}

		case HookFetchRecordUnbuffered:
			if buffered {
				return
			}
			for i := range f.Topics {
				t := &f.Topics[i]
				for j := range t.Partitions {
					p := &t.Partitions[j]
					for _, r := range p.Records {
						h.OnFetchRecordUnbuffered(r, polled)
					}
				}
			}
		}
	})

	var nrecs int
	for i := range f.Topics {
		t := &f.Topics[i]
		for j := range t.Partitions {
			nrecs += len(t.Partitions[j].Records)
		}
	}
	if buffered {
		atomic.AddInt64(&s.cl.consumer.bufferedRecords, int64(nrecs))
	} else {
		atomic.AddInt64(&s.cl.consumer.bufferedRecords, -int64(nrecs))
	}
}

// takeBuffered drains a buffered fetch and updates offsets.
func (s *source) takeBuffered() Fetch {
	return s.takeBufferedFn(true, usedOffsets.finishUsingAllWithSet)
}

func (s *source) discardBuffered() {
	s.takeBufferedFn(false, usedOffsets.finishUsingAll)
}

// takeNBuffered takes a limited amount of records from a buffered fetch,
// updating offsets in each partition per records taken.
//
// This only allows a new fetch once every buffered record has been taken.
//
// This returns the number of records taken and whether the source has been
// completely drained.
func (s *source) takeNBuffered(n int) (Fetch, int, bool) {
	var r Fetch
	var taken int

	b := &s.buffered
	bf := &b.fetch
	for len(bf.Topics) > 0 && n > 0 {
		t := &bf.Topics[0]

		r.Topics = append(r.Topics, *t)
		rt := &r.Topics[len(r.Topics)-1]
		rt.Partitions = nil

		tCursors := b.usedOffsets[t.Topic]

		for len(t.Partitions) > 0 && n > 0 {
			p := &t.Partitions[0]

			rt.Partitions = append(rt.Partitions, *p)
			rp := &rt.Partitions[len(rt.Partitions)-1]

			take := n
			if take > len(p.Records) {
				take = len(p.Records)
			}

			rp.Records = p.Records[:take:take]
			p.Records = p.Records[take:]

			n -= take
			taken += take

			pCursor := tCursors[p.Partition]

			if len(p.Records) == 0 {
				t.Partitions = t.Partitions[1:]

				pCursor.from.setOffset(pCursor.cursorOffset)
				pCursor.from.allowUsable()
				delete(tCursors, p.Partition)
				if len(tCursors) == 0 {
					delete(b.usedOffsets, t.Topic)
				}
				break
			}

			lastReturnedRecord := rp.Records[len(rp.Records)-1]
			pCursor.from.setOffset(cursorOffset{
				offset:            lastReturnedRecord.Offset + 1,
				lastConsumedEpoch: lastReturnedRecord.LeaderEpoch,
				hwm:               p.HighWatermark,
			})
		}

		if len(t.Partitions) == 0 {
			bf.Topics = bf.Topics[1:]
		}
	}

	s.hook(&r, false, true) // unbuffered, polled

	drained := len(bf.Topics) == 0
	if drained {
		s.takeBuffered()
	}
	return r, taken, drained
}

func (s *source) takeBufferedFn(polled bool, offsetFn func(usedOffsets)) Fetch {
	r := s.buffered
	s.buffered = bufferedFetch{}
	offsetFn(r.usedOffsets)
	r.doneFetch <- struct{}{}
	close(s.sem)

	s.hook(&r.fetch, false, polled) // unbuffered, potentially polled

	return r.fetch
}

// createReq actually creates a fetch request.
func (s *source) createReq() *fetchRequest {
	req := &fetchRequest{
		maxWait:        s.cl.cfg.maxWait,
		minBytes:       s.cl.cfg.minBytes,
		maxBytes:       s.cl.cfg.maxBytes,
		maxPartBytes:   s.cl.cfg.maxPartBytes,
		rack:           s.cl.cfg.rack,
		isolationLevel: s.cl.cfg.isolationLevel,
		preferLagFn:    s.cl.cfg.preferLagFn,

		// We copy a view of the session for the request, which allows
		// modify source while the request may be reading its copy.
		session: s.session,
	}

	paused := s.cl.consumer.loadPaused()

	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()

	cursorIdx := s.cursorsStart
	for i := 0; i < len(s.cursors); i++ {
		c := s.cursors[cursorIdx]
		cursorIdx = (cursorIdx + 1) % len(s.cursors)
		if !c.usable() || paused.has(c.topic, c.partition) {
			continue
		}
		req.addCursor(c)
	}

	// We could have lost our only record buffer just before we grabbed the
	// source lock above.
	if len(s.cursors) > 0 {
		s.cursorsStart = (s.cursorsStart + 1) % len(s.cursors)
	}

	return req
}

func (s *source) maybeConsume() {
	if s.fetchState.maybeBegin() {
		go s.loopFetch()
	}
}

func (s *source) loopFetch() {
	consumer := &s.cl.consumer
	session := consumer.loadSession()

	if session == noConsumerSession {
		s.fetchState.hardFinish()
		return
	}

	session.incWorker()
	defer session.decWorker()

	// After our add, check quickly **without** another select case to
	// determine if this context was truly canceled. Any other select that
	// has another select case could theoretically race with the other case
	// also being selected.
	select {
	case <-session.ctx.Done():
		s.fetchState.hardFinish()
		return
	default:
	}

	// We receive on canFetch when we can fetch, and we send back when we
	// are done fetching.
	canFetch := make(chan chan struct{}, 1)

	again := true
	for again {
		select {
		case <-session.ctx.Done():
			s.fetchState.hardFinish()
			return
		case <-s.sem:
		}

		select {
		case <-session.ctx.Done():
			s.fetchState.hardFinish()
			return
		case session.desireFetch() <- canFetch:
		}

		select {
		case <-session.ctx.Done():
			session.cancelFetchCh <- canFetch
			s.fetchState.hardFinish()
			return
		case doneFetch := <-canFetch:
			again = s.fetchState.maybeFinish(s.fetch(session, doneFetch))
		}
	}
}

// fetch is the main logic center of fetching messages.
//
// This is a long function, made much longer by winded documentation, that
// contains a lot of the side effects of fetching and updating. The function
// consists of two main bulks of logic:
//
//   - First, issue a request that can be killed if the source needs to be
//     stopped. Processing the response modifies no state on the source.
//
//   - Second, we keep the fetch response and update everything relevant
//     (session, trigger some list or epoch updates, buffer the fetch).
//
// One small part between the first and second step is to update preferred
// replicas. We always keep the preferred replicas from the fetch response
// *even if* the source needs to be stopped. The knowledge of which preferred
// replica to use would not be out of date even if the consumer session is
// changing.
func (s *source) fetch(consumerSession *consumerSession, doneFetch chan<- struct{}) (fetched bool) {
	req := s.createReq()

	// For all returns, if we do not buffer our fetch, then we want to
	// ensure our used offsets are usable again.
	var (
		alreadySentToDoneFetch bool
		setOffsets             bool
		buffered               bool
	)
	defer func() {
		if !buffered {
			if req.numOffsets > 0 {
				if setOffsets {
					req.usedOffsets.finishUsingAllWithSet()
				} else {
					req.usedOffsets.finishUsingAll()
				}
			}
			if !alreadySentToDoneFetch {
				doneFetch <- struct{}{}
			}
		}
	}()

	if req.numOffsets == 0 { // cursors could have been set unusable
		return
	}

	// If our fetch is killed, we want to cancel waiting for the response.
	var (
		kresp       kmsg.Response
		requested   = make(chan struct{})
		ctx, cancel = context.WithCancel(consumerSession.ctx)
	)
	defer cancel()

	br, err := s.cl.brokerOrErr(ctx, s.nodeID, errUnknownBroker)
	if err != nil {
		close(requested)
	} else {
		br.do(ctx, req, func(k kmsg.Response, e error) {
			kresp, err = k, e
			close(requested)
		})
	}

	select {
	case <-requested:
		fetched = true
	case <-ctx.Done():
		return
	}

	var didBackoff bool
	backoff := func() {
		// We preemptively allow more fetches (since we are not buffering)
		// and reset our session because of the error (who knows if kafka
		// processed the request but the client failed to receive it).
		doneFetch <- struct{}{}
		alreadySentToDoneFetch = true
		s.session.reset()
		didBackoff = true

		s.cl.triggerUpdateMetadata(false, "opportunistic load during source backoff") // as good a time as any
		s.consecutiveFailures++
		after := time.NewTimer(s.cl.cfg.retryBackoff(s.consecutiveFailures))
		defer after.Stop()
		select {
		case <-after.C:
		case <-ctx.Done():
		}
	}
	defer func() {
		if !didBackoff {
			s.consecutiveFailures = 0
		}
	}()

	// If we had an error, we backoff. Killing a fetch quits the backoff,
	// but that is fine; we may just re-request too early and fall into
	// another backoff.
	if err != nil {
		backoff()
		return
	}

	resp := kresp.(*kmsg.FetchResponse)

	var (
		fetch           Fetch
		reloadOffsets   listOrEpochLoads
		preferreds      cursorPreferreds
		allErrsStripped bool
		updateMeta      bool
		updateWhy       string
		handled         = make(chan struct{})
	)

	// Theoretically, handleReqResp could take a bit of CPU time due to
	// decompressing and processing the response. We do this in a goroutine
	// to allow the session to be canceled at any moment.
	//
	// Processing the response only needs the source's nodeID and client.
	go func() {
		defer close(handled)
		fetch, reloadOffsets, preferreds, allErrsStripped, updateMeta, updateWhy = s.handleReqResp(br, req, resp)
	}()

	select {
	case <-handled:
	case <-ctx.Done():
		return
	}

	// The logic below here should be relatively quick.

	deleteReqUsedOffset := func(topic string, partition int32) {
		t := req.usedOffsets[topic]
		delete(t, partition)
		if len(t) == 0 {
			delete(req.usedOffsets, topic)
		}
	}

	// Before updating the source, we move all cursors that have new
	// preferred replicas and remove them from being tracked in our req
	// offsets. We also remove the reload offsets from our req offsets.
	//
	// These two removals transition responsibility for finishing using the
	// cursor from the request's used offsets to the new source or the
	// reloading.
	preferreds.eachPreferred(func(c cursorOffsetPreferred) {
		c.move()
		deleteReqUsedOffset(c.from.topic, c.from.partition)
	})
	reloadOffsets.each(deleteReqUsedOffset)

	// The session on the request was updated; we keep those updates.
	s.session = req.session

	// handleReqResp only parses the body of the response, not the top
	// level error code.
	//
	// The top level error code is related to fetch sessions only, and if
	// there was an error, the body was empty (so processing is basically a
	// no-op). We process the fetch session error now.
	switch err := kerr.ErrorForCode(resp.ErrorCode); err {
	case kerr.FetchSessionIDNotFound:
		if s.session.epoch == 0 {
			// If the epoch was zero, the broker did not even
			// establish a session for us (and thus is maxed on
			// sessions). We stop trying.
			s.cl.cfg.logger.Log(LogLevelInfo, "session failed with SessionIDNotFound while trying to establish a session; broker likely maxed on sessions; continuing on without using sessions", "broker", logID(s.nodeID))
			s.session.kill()
		} else {
			s.cl.cfg.logger.Log(LogLevelInfo, "received SessionIDNotFound from our in use session, our session was likely evicted; resetting session", "broker", logID(s.nodeID))
			s.session.reset()
		}
		return
	case kerr.InvalidFetchSessionEpoch:
		s.cl.cfg.logger.Log(LogLevelInfo, "resetting fetch session", "broker", logID(s.nodeID), "err", err)
		s.session.reset()
		return

	case kerr.FetchSessionTopicIDError, kerr.UnknownTopicID, kerr.InconsistentTopicID:
		s.cl.cfg.logger.Log(LogLevelInfo, "topic id issues, resetting session and updating metadata", "broker", logID(s.nodeID), "err", err)
		s.session.reset()
		s.cl.triggerUpdateMetadataNow("topic id issues")
		return
	}

	// At this point, we have successfully processed the response. Even if
	// the response contains no records, we want to keep any offset
	// advancements (we could have consumed only control records, we must
	// advance past them).
	setOffsets = true

	if resp.Version < 7 || resp.SessionID <= 0 {
		// If the version is less than 7, we cannot use fetch sessions,
		// so we kill them on the first response.
		s.session.kill()
	} else {
		s.session.bumpEpoch(resp.SessionID)
	}

	if updateMeta && !reloadOffsets.loadWithSessionNow(consumerSession, updateWhy) {
		s.cl.triggerUpdateMetadataNow(updateWhy)
	}

	if fetch.hasErrorsOrRecords() {
		buffered = true
		s.buffered = bufferedFetch{
			fetch:       fetch,
			doneFetch:   doneFetch,
			usedOffsets: req.usedOffsets,
		}
		s.sem = make(chan struct{})
		s.hook(&fetch, true, false) // buffered, not polled
		s.cl.consumer.addSourceReadyForDraining(s)
	} else if allErrsStripped {
		// If we stripped all errors from the response, we are likely
		// fetching from topics that were deleted. We want to back off
		// a bit rather than spin-loop immediately re-requesting
		// deleted topics.
		backoff()
	}
	return
}

// Parses a fetch response into a Fetch, offsets to reload, and whether
// metadata needs updating.
//
// This only uses a source's broker and client, and thus does not need
// the source mutex.
//
// This function, and everything it calls, is side effect free.
func (s *source) handleReqResp(br *broker, req *fetchRequest, resp *kmsg.FetchResponse) (
	f Fetch,
	reloadOffsets listOrEpochLoads,
	preferreds cursorPreferreds,
	allErrsStripped bool,
	updateMeta bool,
	why string,
) {
	f = Fetch{Topics: make([]FetchTopic, 0, len(resp.Topics))}
	var (
		updateWhy       multiUpdateWhy
		numErrsStripped int
		kip320          = s.cl.supportsOffsetForLeaderEpoch()
	)

	for _, rt := range resp.Topics {
		topic := rt.Topic
		// v13 only uses topic IDs, so we have to map the response
		// uuid's to our string topics.
		if resp.Version >= 13 {
			topic = req.id2topic[rt.TopicID]
		}

		// We always include all cursors on this source in the fetch;
		// we should not receive any topics or partitions we do not
		// expect.
		topicOffsets, ok := req.usedOffsets[topic]
		if !ok {
			s.cl.cfg.logger.Log(LogLevelWarn, "broker returned topic from fetch that we did not ask for",
				"broker", logID(s.nodeID),
				"topic", topic,
			)
			continue
		}

		fetchTopic := FetchTopic{
			Topic:      topic,
			Partitions: make([]FetchPartition, 0, len(rt.Partitions)),
		}

		for i := range rt.Partitions {
			rp := &rt.Partitions[i]
			partition := rp.Partition
			partOffset, ok := topicOffsets[partition]
			if !ok {
				s.cl.cfg.logger.Log(LogLevelWarn, "broker returned partition from fetch that we did not ask for",
					"broker", logID(s.nodeID),
					"topic", topic,
					"partition", partition,
				)
				continue
			}

			// If we are fetching from the replica already, Kafka replies with a -1
			// preferred read replica. If Kafka replies with a preferred replica,
			// it sends no records.
			if preferred := rp.PreferredReadReplica; resp.Version >= 11 && preferred >= 0 {
				preferreds = append(preferreds, cursorOffsetPreferred{
					*partOffset,
					preferred,
				})
				continue
			}

			fp := partOffset.processRespPartition(br, rp, s.cl.decompressor, s.cl.cfg.hooks)
			if fp.Err != nil {
				updateMeta = true
				updateWhy.add(topic, partition, fp.Err)
			}

			// We only keep the partition if it has no error, or an
			// error we do not internally retry.
			var keep bool
			switch fp.Err {
			default:
				// - bad auth
				// - unsupported compression
				// - unsupported message version
				// - unknown error
				// - or, no error
				keep = true

			case kerr.UnknownTopicOrPartition,
				kerr.NotLeaderForPartition,
				kerr.ReplicaNotAvailable,
				kerr.KafkaStorageError,
				kerr.UnknownLeaderEpoch, // our meta is newer than broker we fetched from
				kerr.OffsetNotAvailable: // fetched from out of sync replica or a behind in-sync one (KIP-392: case 1 and case 2)

				numErrsStripped++

			case kerr.OffsetOutOfRange:
				// If we are out of range, we reset to what we can.
				// With Kafka >= 2.1, we should only get offset out
				// of range if we fetch before the start, but a user
				// could start past the end and want to reset to
				// the end. We respect that.
				//
				// KIP-392 (case 3) specifies that if we are consuming
				// from a follower, then if our offset request is before
				// the low watermark, we list offsets from the follower.
				//
				// KIP-392 (case 4) specifies that if we are consuming
				// a follower and our request is larger than the high
				// watermark, then we should first check for truncation
				// from the leader and then if we still get out of
				// range, reset with list offsets.
				//
				// It further goes on to say that "out of range errors
				// due to ISR propagation delays should be extremely
				// rare". Rather than falling back to listing offsets,
				// we stay in a cycle of validating the leader epoch
				// until the follower has caught up.
				//
				// In all cases except case 4, we also have to check if
				// no reset offset was configured. If so, we ignore
				// trying to reset and instead keep our failed partition.
				addList := func(replica int32) {
					if s.cl.cfg.resetOffset.noReset {
						keep = true
					} else {
						reloadOffsets.addLoad(topic, partition, loadTypeList, offsetLoad{
							replica: replica,
							Offset:  s.cl.cfg.resetOffset,
						})
					}
				}

				switch {
				case s.nodeID == partOffset.from.leader: // non KIP-392 case
					addList(-1)

				case partOffset.offset < fp.LogStartOffset: // KIP-392 case 3
					addList(s.nodeID)

				default: // partOffset.offset > fp.HighWatermark, KIP-392 case 4
					if kip320 {
						reloadOffsets.addLoad(topic, partition, loadTypeEpoch, offsetLoad{
							replica: -1,
							Offset: Offset{
								at:    partOffset.offset,
								epoch: partOffset.lastConsumedEpoch,
							},
						})
					} else {
						// If the broker does not support offset for leader epoch but
						// does support follower fetching for some reason, we have to
						// fallback to listing.
						addList(-1)
					}
				}

			case kerr.FencedLeaderEpoch:
				// With fenced leader epoch, we notify an error only
				// if necessary after we find out if loss occurred.
				// If we have consumed nothing, then we got unlucky
				// by being fenced right after we grabbed metadata.
				// We just refresh metadata and try again.
				//
				// It would be odd for a broker to reply we are fenced
				// but not support offset for leader epoch, so we do
				// not check KIP-320 support here.
				if partOffset.lastConsumedEpoch >= 0 {
					reloadOffsets.addLoad(topic, partition, loadTypeEpoch, offsetLoad{
						replica: -1,
						Offset: Offset{
							at:    partOffset.offset,
							epoch: partOffset.lastConsumedEpoch,
						},
					})
				}
			}

			if keep {
				fetchTopic.Partitions = append(fetchTopic.Partitions, fp)
			}
		}

		if len(fetchTopic.Partitions) > 0 {
			f.Topics = append(f.Topics, fetchTopic)
		}
	}

	return f, reloadOffsets, preferreds, req.numOffsets == numErrsStripped, updateMeta, updateWhy.reason("fetch had inner topic errors")
}

// processRespPartition processes all records in all potentially compressed
// batches (or message sets).
func (o *cursorOffsetNext) processRespPartition(br *broker, rp *kmsg.FetchResponseTopicPartition, decompressor *decompressor, hooks hooks) FetchPartition {
	fp := FetchPartition{
		Partition:        rp.Partition,
		Err:              kerr.ErrorForCode(rp.ErrorCode),
		HighWatermark:    rp.HighWatermark,
		LastStableOffset: rp.LastStableOffset,
		LogStartOffset:   rp.LogStartOffset,
	}
	if rp.ErrorCode == 0 {
		o.hwm = rp.HighWatermark
	}

	aborter := buildAborter(rp)

	// A response could contain any of message v0, message v1, or record
	// batches, and this is solely dictated by the magic byte (not the
	// fetch response version). The magic byte is located at byte 17.
	//
	// 1 thru 8: int64 offset / first offset
	// 9 thru 12: int32 length
	// 13 thru 16: crc (magic 0 or 1), or partition leader epoch (magic 2)
	// 17: magic
	//
	// We decode and validate similarly for messages and record batches, so
	// we "abstract" away the high level stuff into a check function just
	// below, and then switch based on the magic for how to process.
	var (
		in = rp.RecordBatches

		r           readerFrom
		kind        string
		length      int32
		lengthField *int32
		crcField    *int32
		crcTable    *crc32.Table
		crcAt       int

		check = func() bool {
			// If we call into check, we know we have a valid
			// length, so we should be at least able to parse our
			// top level struct and validate the length and CRC.
			if err := r.ReadFrom(in[:length]); err != nil {
				fp.Err = fmt.Errorf("unable to read %s, not enough data", kind)
				return false
			}
			if length := int32(len(in[12:length])); length != *lengthField {
				fp.Err = fmt.Errorf("encoded length %d does not match read length %d", *lengthField, length)
				return false
			}
			if crcCalc := int32(crc32.Checksum(in[crcAt:length], crcTable)); crcCalc != *crcField {
				fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", *crcField, crcCalc)
				return false
			}
			return true
		}
	)

	for len(in) > 17 && fp.Err == nil {
		offset := int64(binary.BigEndian.Uint64(in))
		length = int32(binary.BigEndian.Uint32(in[8:]))
		length += 12 // for the int64 offset we skipped and int32 length field itself
		if len(in) < int(length) {
			break
		}

		switch magic := in[16]; magic {
		case 0:
			m := new(kmsg.MessageV0)
			kind = "message v0"
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 1:
			m := new(kmsg.MessageV1)
			kind = "message v1"
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 2:
			rb := new(kmsg.RecordBatch)
			kind = "record batch"
			lengthField = &rb.Length
			crcField = &rb.CRC
			crcTable = crc32c
			crcAt = 21
			r = rb

		default:
			fp.Err = fmt.Errorf("unknown magic %d; message offset is %d and length is %d, skipping and setting to next offset", magic, offset, length)
			if next := offset + 1; next > o.offset {
				o.offset = next
			}
			return fp
		}

		if !check() {
			break
		}

		in = in[length:]

		var m FetchBatchMetrics

		switch t := r.(type) {
		case *kmsg.MessageV0:
			m.CompressedBytes = int(length) // for message sets, we include the message set overhead in length
			m.CompressionType = uint8(t.Attributes) & 0b0000_0111
			m.NumRecords, m.UncompressedBytes = o.processV0OuterMessage(&fp, t, decompressor)

		case *kmsg.MessageV1:
			m.CompressedBytes = int(length)
			m.CompressionType = uint8(t.Attributes) & 0b0000_0111
			m.NumRecords, m.UncompressedBytes = o.processV1OuterMessage(&fp, t, decompressor)

		case *kmsg.RecordBatch:
			m.CompressedBytes = len(t.Records) // for record batches, we only track the record batch length
			m.CompressionType = uint8(t.Attributes) & 0b0000_0111
			m.NumRecords, m.UncompressedBytes = o.processRecordBatch(&fp, t, aborter, decompressor)
		}

		if m.UncompressedBytes == 0 {
			m.UncompressedBytes = m.CompressedBytes
		}
		hooks.each(func(h Hook) {
			if h, ok := h.(HookFetchBatchRead); ok {
				h.OnFetchBatchRead(br.meta, o.from.topic, o.from.partition, m)
			}
		})
	}

	return fp
}

type aborter map[int64][]int64

func buildAborter(rp *kmsg.FetchResponseTopicPartition) aborter {
	if len(rp.AbortedTransactions) == 0 {
		return nil
	}
	a := make(aborter)
	for _, abort := range rp.AbortedTransactions {
		a[abort.ProducerID] = append(a[abort.ProducerID], abort.FirstOffset)
	}
	return a
}

func (a aborter) shouldAbortBatch(b *kmsg.RecordBatch) bool {
	if len(a) == 0 || b.Attributes&0b0001_0000 == 0 {
		return false
	}
	pidAborts := a[b.ProducerID]
	if len(pidAborts) == 0 {
		return false
	}
	// If the first offset in this batch is less than the first offset
	// aborted, then this batch is not aborted.
	if b.FirstOffset < pidAborts[0] {
		return false
	}
	return true
}

func (a aborter) trackAbortedPID(producerID int64) {
	remaining := a[producerID][1:]
	if len(remaining) == 0 {
		delete(a, producerID)
	} else {
		a[producerID] = remaining
	}
}

//////////////////////////////////////
// processing records to fetch part //
//////////////////////////////////////

// readRawRecords reads n records from in and returns them, returning early if
// there were partial records.
func readRawRecords(n int, in []byte) []kmsg.Record {
	rs := make([]kmsg.Record, n)
	for i := 0; i < n; i++ {
		length, used := kbin.Varint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return rs[:i]
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			return rs[:i]
		}
		in = in[total:]
	}
	return rs
}

func (o *cursorOffsetNext) processRecordBatch(
	fp *FetchPartition,
	batch *kmsg.RecordBatch,
	aborter aborter,
	decompressor *decompressor,
) (int, int) {
	if batch.Magic != 2 {
		fp.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return 0, 0
	}
	lastOffset := batch.FirstOffset + int64(batch.LastOffsetDelta)
	if lastOffset < o.offset {
		// If the last offset in this batch is less than what we asked
		// for, we got a batch that we entirely do not need. We can
		// avoid all work (although we should not get this batch).
		return 0, 0
	}

	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		if rawRecords, err = decompressor.decompress(rawRecords, compression); err != nil {
			return 0, 0 // truncated batch
		}
	}

	uncompressedBytes := len(rawRecords)

	numRecords := int(batch.NumRecords)
	krecords := readRawRecords(numRecords, rawRecords)

	// KAFKA-5443: compacted topics preserve the last offset in a batch,
	// even if the last record is removed, meaning that using offsets from
	// records alone may not get us to the next offset we need to ask for.
	//
	// We only perform this logic if we did not consume a truncated batch.
	// If we consume a truncated batch, then what was truncated could have
	// been an offset we are interested in consuming. Even if our fetch did
	// not advance this partition at all, we will eventually fetch from the
	// partition and not have a truncated response, at which point we will
	// either advance offsets or will set to nextAskOffset.
	nextAskOffset := lastOffset + 1
	defer func() {
		if numRecords == len(krecords) && o.offset < nextAskOffset {
			o.offset = nextAskOffset
		}
	}()

	abortBatch := aborter.shouldAbortBatch(batch)
	for i := range krecords {
		record := recordToRecord(
			o.from.topic,
			fp.Partition,
			batch,
			&krecords[i],
		)
		o.maybeKeepRecord(fp, record, abortBatch)

		if abortBatch && record.Attrs.IsControl() {
			// A control record has a key and a value where the key
			// is int16 version and int16 type. Aborted records
			// have a type of 0.
			if key := record.Key; len(key) >= 4 && key[2] == 0 && key[3] == 0 {
				aborter.trackAbortedPID(batch.ProducerID)
			}
		}
	}

	return len(krecords), uncompressedBytes
}

// Processes an outer v1 message. There could be no inner message, which makes
// this easy, but if not, we decompress and process each inner message as
// either v0 or v1. We only expect the inner message to be v1, but technically
// a crazy pipeline could have v0 anywhere.
func (o *cursorOffsetNext) processV1OuterMessage(
	fp *FetchPartition,
	message *kmsg.MessageV1,
	decompressor *decompressor,
) (int, int) {
	compression := byte(message.Attributes & 0x0003)
	if compression == 0 {
		o.processV1Message(fp, message)
		return 1, 0
	}

	rawInner, err := decompressor.decompress(message.Value, compression)
	if err != nil {
		return 0, 0 // truncated batch
	}

	uncompressedBytes := len(rawInner)

	var innerMessages []readerFrom
out:
	for len(rawInner) > 17 { // magic at byte 17
		length := int32(binary.BigEndian.Uint32(rawInner[8:]))
		length += 12 // offset and length fields
		if len(rawInner) < int(length) {
			break
		}

		var (
			magic = rawInner[16]

			msg         readerFrom
			lengthField *int32
			crcField    *int32
		)

		switch magic {
		case 0:
			m := new(kmsg.MessageV0)
			msg = m
			lengthField = &m.MessageSize
			crcField = &m.CRC
		case 1:
			m := new(kmsg.MessageV1)
			msg = m
			lengthField = &m.MessageSize
			crcField = &m.CRC

		default:
			fp.Err = fmt.Errorf("message set v1 has inner message with invalid magic %d", magic)
			break out
		}

		if err := msg.ReadFrom(rawInner[:length]); err != nil {
			fp.Err = fmt.Errorf("unable to read message v%d, not enough data", magic)
			break
		}
		if length := int32(len(rawInner[12:length])); length != *lengthField {
			fp.Err = fmt.Errorf("encoded length %d does not match read length %d", *lengthField, length)
			break
		}
		if crcCalc := int32(crc32.ChecksumIEEE(rawInner[16:length])); crcCalc != *crcField {
			fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", *crcField, crcCalc)
			break
		}
		innerMessages = append(innerMessages, msg)
		rawInner = rawInner[length:]
	}
	if len(innerMessages) == 0 {
		return 0, uncompressedBytes
	}

	firstOffset := message.Offset - int64(len(innerMessages)) + 1
	for i := range innerMessages {
		innerMessage := innerMessages[i]
		switch innerMessage := innerMessage.(type) {
		case *kmsg.MessageV0:
			innerMessage.Offset = firstOffset + int64(i)
			innerMessage.Attributes |= int8(compression)
			if !o.processV0Message(fp, innerMessage) {
				return i, uncompressedBytes
			}
		case *kmsg.MessageV1:
			innerMessage.Offset = firstOffset + int64(i)
			innerMessage.Attributes |= int8(compression)
			if !o.processV1Message(fp, innerMessage) {
				return i, uncompressedBytes
			}
		}
	}
	return len(innerMessages), uncompressedBytes
}

func (o *cursorOffsetNext) processV1Message(
	fp *FetchPartition,
	message *kmsg.MessageV1,
) bool {
	if message.Magic != 1 {
		fp.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if uint8(message.Attributes)&0b1111_0000 != 0 {
		fp.Err = fmt.Errorf("unknown attributes on message %d", message.Attributes)
		return false
	}
	record := v1MessageToRecord(o.from.topic, fp.Partition, message)
	o.maybeKeepRecord(fp, record, false)
	return true
}

// Processes an outer v0 message. We expect inner messages to be entirely v0 as
// well, so this only tries v0 always.
func (o *cursorOffsetNext) processV0OuterMessage(
	fp *FetchPartition,
	message *kmsg.MessageV0,
	decompressor *decompressor,
) (int, int) {
	compression := byte(message.Attributes & 0x0003)
	if compression == 0 {
		o.processV0Message(fp, message)
		return 1, 0 // uncompressed bytes is 0; set to compressed bytes on return
	}

	rawInner, err := decompressor.decompress(message.Value, compression)
	if err != nil {
		return 0, 0 // truncated batch
	}

	uncompressedBytes := len(rawInner)

	var innerMessages []kmsg.MessageV0
	for len(rawInner) > 17 { // magic at byte 17
		length := int32(binary.BigEndian.Uint32(rawInner[8:]))
		length += 12 // offset and length fields
		if len(rawInner) < int(length) {
			break // truncated batch
		}
		var m kmsg.MessageV0
		if err := m.ReadFrom(rawInner[:length]); err != nil {
			fp.Err = fmt.Errorf("unable to read message v0, not enough data")
			break
		}
		if length := int32(len(rawInner[12:length])); length != m.MessageSize {
			fp.Err = fmt.Errorf("encoded length %d does not match read length %d", m.MessageSize, length)
			break
		}
		if crcCalc := int32(crc32.ChecksumIEEE(rawInner[16:length])); crcCalc != m.CRC {
			fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", m.CRC, crcCalc)
			break
		}
		innerMessages = append(innerMessages, m)
		rawInner = rawInner[length:]
	}
	if len(innerMessages) == 0 {
		return 0, uncompressedBytes
	}

	firstOffset := message.Offset - int64(len(innerMessages)) + 1
	for i := range innerMessages {
		innerMessage := &innerMessages[i]
		innerMessage.Attributes |= int8(compression)
		innerMessage.Offset = firstOffset + int64(i)
		if !o.processV0Message(fp, innerMessage) {
			return i, uncompressedBytes
		}
	}
	return len(innerMessages), uncompressedBytes
}

func (o *cursorOffsetNext) processV0Message(
	fp *FetchPartition,
	message *kmsg.MessageV0,
) bool {
	if message.Magic != 0 {
		fp.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if uint8(message.Attributes)&0b1111_1000 != 0 {
		fp.Err = fmt.Errorf("unknown attributes on message %d", message.Attributes)
		return false
	}
	record := v0MessageToRecord(o.from.topic, fp.Partition, message)
	o.maybeKeepRecord(fp, record, false)
	return true
}

// maybeKeepRecord keeps a record if it is within our range of offsets to keep.
//
// If the record is being aborted or the record is a control record and the
// client does not want to keep control records, this does not keep the record.
func (o *cursorOffsetNext) maybeKeepRecord(fp *FetchPartition, record *Record, abort bool) {
	if record.Offset < o.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}

	// We only keep control records if specifically requested.
	if record.Attrs.IsControl() {
		abort = !o.from.keepControl
	}
	if !abort {
		fp.Records = append(fp.Records, record)
	}

	// The record offset may be much larger than our expected offset if the
	// topic is compacted.
	o.offset = record.Offset + 1
	o.lastConsumedEpoch = record.LeaderEpoch
}

///////////////////////////////
// kmsg.Record to kgo.Record //
///////////////////////////////

func timeFromMillis(millis int64) time.Time {
	return time.Unix(0, millis*1e6)
}

// recordToRecord converts a kmsg.RecordBatch's Record to a kgo Record.
func recordToRecord(
	topic string,
	partition int32,
	batch *kmsg.RecordBatch,
	record *kmsg.Record,
) *Record {
	h := make([]RecordHeader, 0, len(record.Headers))
	for _, kv := range record.Headers {
		h = append(h, RecordHeader{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	r := &Record{
		Key:           record.Key,
		Value:         record.Value,
		Headers:       h,
		Topic:         topic,
		Partition:     partition,
		Attrs:         RecordAttrs{uint8(batch.Attributes)},
		ProducerID:    batch.ProducerID,
		ProducerEpoch: batch.ProducerEpoch,
		LeaderEpoch:   batch.PartitionLeaderEpoch,
		Offset:        batch.FirstOffset + int64(record.OffsetDelta),
	}
	if r.Attrs.TimestampType() == 0 {
		r.Timestamp = timeFromMillis(batch.FirstTimestamp + int64(record.TimestampDelta))
	} else {
		r.Timestamp = timeFromMillis(batch.MaxTimestamp)
	}
	return r
}

func messageAttrsToRecordAttrs(attrs int8, v0 bool) RecordAttrs {
	uattrs := uint8(attrs)
	if v0 {
		uattrs |= 0b1000_0000
	}
	return RecordAttrs{uattrs}
}

func v0MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV0,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		Topic:         topic,
		Partition:     partition,
		Attrs:         messageAttrsToRecordAttrs(message.Attributes, true),
		ProducerID:    -1,
		ProducerEpoch: -1,
		LeaderEpoch:   -1,
		Offset:        message.Offset,
	}
}

func v1MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV1,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		Timestamp:     timeFromMillis(message.Timestamp),
		Topic:         topic,
		Partition:     partition,
		Attrs:         messageAttrsToRecordAttrs(message.Attributes, false),
		ProducerID:    -1,
		ProducerEpoch: -1,
		LeaderEpoch:   -1,
		Offset:        message.Offset,
	}
}

//////////////////
// fetchRequest //
//////////////////

type fetchRequest struct {
	version      int16
	maxWait      int32
	minBytes     int32
	maxBytes     int32
	maxPartBytes int32
	rack         string

	isolationLevel int8
	preferLagFn    PreferLagFn

	numOffsets  int
	usedOffsets usedOffsets

	torder []string           // order of topics to write
	porder map[string][]int32 // per topic, order of partitions to write

	topic2id map[string][16]byte
	id2topic map[[16]byte]string

	// Session is a copy of the source session at the time a request is
	// built. If the source is reset, the session it has is reset at the
	// field level only. Our view of the original session is still valid.
	session fetchSession
}

func (f *fetchRequest) addCursor(c *cursor) {
	if f.usedOffsets == nil {
		f.usedOffsets = make(usedOffsets)
		f.id2topic = make(map[[16]byte]string)
		f.topic2id = make(map[string][16]byte)
		f.porder = make(map[string][]int32)
	}
	partitions := f.usedOffsets[c.topic]
	if partitions == nil {
		partitions = make(map[int32]*cursorOffsetNext)
		f.usedOffsets[c.topic] = partitions
		f.id2topic[c.topicID] = c.topic
		f.topic2id[c.topic] = c.topicID
		f.torder = append(f.torder, c.topic)
	}
	partitions[c.partition] = c.use()
	f.porder[c.topic] = append(f.porder[c.topic], c.partition)
	f.numOffsets++
}

// PreferLagFn accepts topic and partition lag, the previously determined topic
// order, and the previously determined per-topic partition order, and returns
// a new topic and per-topic partition order.
//
// Most use cases will not need to look at the prior orders, but they exist
// if you want to get fancy.
//
// You can return partial results: if you only return topics, partitions within
// each topic keep their prior ordering. If you only return some topics but not
// all, the topics you do not return / the partitions you do not return will
// retain their original ordering *after* your given ordering.
type PreferLagFn func(lag map[string]map[int32]int64, torderPrior []string, porderPrior map[string][]int32) ([]string, map[string][]int32)

// PreferLagAt is a simple PreferLagFn that orders the largest lag first, for
// any topic that is collectively lagging more than preferLagAt, and for any
// partition that is lagging more than preferLagAt.
//
// The function does not prescribe any ordering for topics that have the same
// lag. It is recommended to use a number more than 0 or 1: if you use 0, you
// may just always undo client ordering when there is no actual lag.
func PreferLagAt(preferLagAt int64) PreferLagFn {
	if preferLagAt < 0 {
		return nil
	}
	return func(lag map[string]map[int32]int64, _ []string, _ map[string][]int32) ([]string, map[string][]int32) {
		type plag struct {
			p   int32
			lag int64
		}
		type tlag struct {
			t   string
			lag int64
			ps  []plag
		}

		// First, collect all partition lag into per-topic lag.
		tlags := make(map[string]tlag, len(lag))
		for t, ps := range lag {
			for p, lag := range ps {
				prior := tlags[t]
				tlags[t] = tlag{
					t:   t,
					lag: prior.lag + lag,
					ps:  append(prior.ps, plag{p, lag}),
				}
			}
		}

		// We now remove topics and partitions that are not lagging
		// enough. Collectively, the topic could be lagging too much,
		// but individually, no partition is lagging that much: we will
		// sort the topic first and keep the old partition ordering.
		for t, tlag := range tlags {
			if tlag.lag < preferLagAt {
				delete(tlags, t)
				continue
			}
			for i := 0; i < len(tlag.ps); i++ {
				plag := tlag.ps[i]
				if plag.lag < preferLagAt {
					tlag.ps[i] = tlag.ps[len(tlag.ps)-1]
					tlag.ps = tlag.ps[:len(tlag.ps)-1]
					i--
				}
			}
		}
		if len(tlags) == 0 {
			return nil, nil
		}

		var sortedLags []tlag
		for _, tlag := range tlags {
			sort.Slice(tlag.ps, func(i, j int) bool { return tlag.ps[i].lag > tlag.ps[j].lag })
			sortedLags = append(sortedLags, tlag)
		}
		sort.Slice(sortedLags, func(i, j int) bool { return sortedLags[i].lag > sortedLags[j].lag })

		// We now return our laggy topics and partitions, and let the
		// caller add back any missing topics / partitions in their
		// prior order.
		torder := make([]string, 0, len(sortedLags))
		for _, t := range sortedLags {
			torder = append(torder, t.t)
		}
		porder := make(map[string][]int32, len(sortedLags))
		for _, tlag := range sortedLags {
			ps := make([]int32, 0, len(tlag.ps))
			for _, p := range tlag.ps {
				ps = append(ps, p.p)
			}
			porder[tlag.t] = ps
		}
		return torder, porder
	}
}

// If the end user prefers to consume lag, we reorder our previously ordered
// partitions, preferring first the laggiest topics, and then within those, the
// laggiest partitions.
func (f *fetchRequest) adjustPreferringLag() {
	if f.preferLagFn == nil {
		return
	}

	tall := make(map[string]struct{}, len(f.torder))
	for _, t := range f.torder {
		tall[t] = struct{}{}
	}
	pall := make(map[string][]int32, len(f.porder))
	for t, ps := range f.porder {
		pall[t] = append([]int32(nil), ps...)
	}

	lag := make(map[string]map[int32]int64, len(f.torder))
	for t, ps := range f.usedOffsets {
		plag := make(map[int32]int64, len(ps))
		lag[t] = plag
		for p, c := range ps {
			hwm := c.hwm
			if c.hwm < 0 {
				hwm = 0
			}
			lag := hwm - c.offset
			if c.offset <= 0 {
				lag = hwm
			}
			plag[p] = lag
		}
	}

	torder, porder := f.preferLagFn(lag, f.torder, f.porder)
	if torder == nil && porder == nil {
		return
	}
	if torder == nil {
		torder = f.torder
	}
	if porder == nil {
		porder = f.porder
	}
	defer func() { f.torder, f.porder = torder, porder }()

	// Remove any extra topics the user returned that we were not
	// consuming, and add all topics they did not give back.
	for i := 0; i < len(torder); i++ {
		t := torder[i]
		if _, exists := tall[t]; !exists {
			torder = append(torder[:i], torder[i+1:]...) // user gave topic we were not fetching
			i--
		}
		delete(tall, t)
	}
	for t := range tall {
		torder = append(torder, t) // user did not return topic we were fetching
	}

	// Now, same thing for partitions.
	pused := make(map[int32]struct{})
	for t, ps := range pall {
		order, exists := porder[t]
		if !exists {
			porder[t] = ps // shortcut: user did not define this partition's oorder, keep old order
			continue
		}
		for _, p := range ps {
			pused[p] = struct{}{}
		}
		for i := 0; i < len(order); i++ {
			p := order[i]
			if _, exists := pused[p]; !exists {
				order = append(order[:i], order[i+1:]...)
				i--
			}
			delete(pused, p)
		}
		for p := range pused {
			order = append(order, p)
		}
		porder[t] = order
	}
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 13 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) IsFlexible() bool   { return f.version >= 12 } // version 12+ is flexible
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.NewFetchRequest()
	req.Version = f.version
	req.ReplicaID = -1
	req.MaxWaitMillis = f.maxWait
	req.MinBytes = f.minBytes
	req.MaxBytes = f.maxBytes
	req.IsolationLevel = f.isolationLevel
	req.SessionID = f.session.id
	req.SessionEpoch = f.session.epoch
	req.Rack = f.rack

	// We track which partitions we add in this request; any partitions
	// missing that are already in the session get added to forgotten
	// topics at the end.
	var sessionUsed map[string]map[int32]struct{}
	if !f.session.killed {
		sessionUsed = make(map[string]map[int32]struct{}, len(f.usedOffsets))
	}

	f.adjustPreferringLag()

	for _, topic := range f.torder {
		partitions := f.usedOffsets[topic]

		var reqTopic *kmsg.FetchRequestTopic
		sessionTopic := f.session.lookupTopic(topic)

		var usedTopic map[int32]struct{}
		if sessionUsed != nil {
			usedTopic = make(map[int32]struct{}, len(partitions))
		}

		for _, partition := range f.porder[topic] {
			cursorOffsetNext := partitions[partition]

			if usedTopic != nil {
				usedTopic[partition] = struct{}{}
			}

			if !sessionTopic.hasPartitionAt(
				partition,
				cursorOffsetNext.offset,
				cursorOffsetNext.currentLeaderEpoch,
			) {
				if reqTopic == nil {
					t := kmsg.NewFetchRequestTopic()
					t.Topic = topic
					t.TopicID = f.topic2id[topic]
					req.Topics = append(req.Topics, t)
					reqTopic = &req.Topics[len(req.Topics)-1]
				}

				reqPartition := kmsg.NewFetchRequestTopicPartition()
				reqPartition.Partition = partition
				reqPartition.CurrentLeaderEpoch = cursorOffsetNext.currentLeaderEpoch
				reqPartition.FetchOffset = cursorOffsetNext.offset
				reqPartition.LastFetchedEpoch = -1
				reqPartition.LogStartOffset = -1
				reqPartition.PartitionMaxBytes = f.maxPartBytes
				reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
			}
		}

		if sessionUsed != nil {
			sessionUsed[topic] = usedTopic
		}
	}

	// Now for everything that we did not use in our session, add it to
	// forgotten topics and remove it from the session.
	if sessionUsed != nil {
		for topic, partitions := range f.session.used {
			var forgottenTopic *kmsg.FetchRequestForgottenTopic
			topicUsed := sessionUsed[topic]
			for partition := range partitions {
				if topicUsed != nil {
					if _, partitionUsed := topicUsed[partition]; partitionUsed {
						continue
					}
				}
				if forgottenTopic == nil {
					t := kmsg.NewFetchRequestForgottenTopic()
					t.Topic = topic
					t.TopicID = f.topic2id[topic]
					req.ForgottenTopics = append(req.ForgottenTopics, t)
					forgottenTopic = &req.ForgottenTopics[len(req.ForgottenTopics)-1]
				}
				forgottenTopic.Partitions = append(forgottenTopic.Partitions, partition)
				delete(partitions, partition)
			}
			if len(partitions) == 0 {
				delete(f.session.used, topic)
			}
		}
	}

	return req.AppendTo(dst)
}

func (*fetchRequest) ReadFrom([]byte) error {
	panic("unreachable -- the client never uses ReadFrom on its internal fetchRequest")
}

func (f *fetchRequest) ResponseKind() kmsg.Response {
	r := kmsg.NewPtrFetchResponse()
	r.Version = f.version
	return r
}

// fetchSessions, introduced in KIP-227, allow us to send less information back
// and forth to a Kafka broker.
type fetchSession struct {
	id    int32
	epoch int32

	used map[string]map[int32]fetchSessionOffsetEpoch // what we have in the session so far

	killed bool // if we cannot use a session anymore
}

func (s *fetchSession) kill() {
	s.id = 0
	s.epoch = -1
	s.used = nil
	s.killed = true
}

// reset resets the session by setting the next request to use epoch 0.
// We do not reset the ID; using epoch 0 for an existing ID unregisters the
// prior session.
func (s *fetchSession) reset() {
	if s.killed {
		return
	}
	s.epoch = 0
	s.used = nil
}

// bumpEpoch bumps the epoch and saves the session id.
//
// Kafka replies with the session ID of the session to use. When it does, we
// start from epoch 1, wrapping back to 1 if we go negative.
func (s *fetchSession) bumpEpoch(id int32) {
	if s.killed {
		return
	}
	if id != s.id {
		s.epoch = 0 // new session: reset to 0 for the increment below
	}
	s.epoch++
	if s.epoch < 0 {
		s.epoch = 1 // we wrapped: reset back to 1 to continue this session
	}
	s.id = id
}

func (s *fetchSession) lookupTopic(topic string) fetchSessionTopic {
	if s.killed {
		return nil
	}
	if s.used == nil {
		s.used = make(map[string]map[int32]fetchSessionOffsetEpoch)
	}
	t := s.used[topic]
	if t == nil {
		t = make(map[int32]fetchSessionOffsetEpoch)
		s.used[topic] = t
	}
	return t
}

type fetchSessionOffsetEpoch struct {
	offset int64
	epoch  int32
}

type fetchSessionTopic map[int32]fetchSessionOffsetEpoch

func (s fetchSessionTopic) hasPartitionAt(partition int32, offset int64, epoch int32) bool {
	if s == nil { // if we are nil, the session was killed
		return false
	}
	at, exists := s[partition]
	now := fetchSessionOffsetEpoch{offset, epoch}
	s[partition] = now
	return exists && at == now
}
