package kgo

type directConsumer struct {
	cfg    *cfg
	tps    *topicsPartitions             // data for topics that the user assigned
	reSeen map[string]bool               // topics we evaluated against regex, and whether we want them or not
	using  map[string]map[int32]struct{} // topics we are currently using (this only grows)
}

func (c *consumer) initDirect() {
	d := &directConsumer{
		cfg:    &c.cl.cfg,
		tps:    newTopicsPartitions(),
		reSeen: make(map[string]bool),
		using:  make(map[string]map[int32]struct{}),
	}
	c.d = d

	if d.cfg.regex {
		return
	}

	var topics []string
	for topic := range d.cfg.topics {
		topics = append(topics, topic)
	}
	for topic := range d.cfg.partitions {
		topics = append(topics, topic)
	}
	d.tps.storeTopics(topics) // prime topics to load if non-regex (this is of no benefit if regex)
}

// For SetOffsets, unlike the group consumer, we just blindly translate the
// input EpochOffsets into Offsets, and those will be set directly.
func (*directConsumer) getSetAssigns(setOffsets map[string]map[int32]EpochOffset) (assigns map[string]map[int32]Offset) {
	assigns = make(map[string]map[int32]Offset)
	for topic, partitions := range setOffsets {
		set := make(map[int32]Offset)
		for partition, eo := range partitions {
			set[partition] = Offset{
				at:    eo.Offset,
				epoch: eo.Epoch,
			}
		}
		assigns[topic] = set
	}
	return assigns
}

// findNewAssignments returns new partitions to consume at given offsets
// based off the current topics.
func (d *directConsumer) findNewAssignments() map[string]map[int32]Offset {
	topics := d.tps.load()

	var rns reNews
	if d.cfg.regex {
		defer rns.log(d.cfg)
	}

	toUse := make(map[string]map[int32]Offset, 10)
	for topic, topicPartitions := range topics {
		// If we are using regex topics, we have to check all
		// topic regexes to see if any match on this topic.
		var useTopic bool
		if d.cfg.regex {
			want, seen := d.reSeen[topic]
			if !seen {
				for rawRe, re := range d.cfg.topics {
					if want = re.MatchString(topic); want {
						rns.add(rawRe, topic)
						break
					}
				}
				if !want {
					rns.skip(topic)
				}
				d.reSeen[topic] = want
			}
			useTopic = want
		} else {
			_, useTopic = d.cfg.topics[topic]
		}

		// If the above detected that we want to keep this topic, we
		// set all partitions as usable.
		//
		// For internal partitions, we only allow consuming them if
		// the topic is explicitly specified.
		if useTopic {
			partitions := topicPartitions.load()
			if d.cfg.regex && partitions.isInternal {
				continue
			}
			toUseTopic := make(map[int32]Offset, len(partitions.partitions))
			for partition := range partitions.partitions {
				toUseTopic[int32(partition)] = d.cfg.resetOffset
			}
			toUse[topic] = toUseTopic
		}

		// Lastly, if this topic has some specific partitions pinned,
		// we set those.
		for partition, offset := range d.cfg.partitions[topic] {
			toUseTopic, exists := toUse[topic]
			if !exists {
				toUseTopic = make(map[int32]Offset, 10)
				toUse[topic] = toUseTopic
			}
			toUseTopic[partition] = offset
		}
	}

	// With everything we want to consume, remove what we are already.
	for topic, partitions := range d.using {
		toUseTopic, exists := toUse[topic]
		if !exists {
			continue // metadata update did not return this topic (regex or failing load)
		}
		for partition := range partitions {
			delete(toUseTopic, partition)
		}
		if len(toUseTopic) == 0 {
			delete(toUse, topic)
		}
	}

	if len(toUse) == 0 {
		return nil
	}

	// Finally, toUse contains new partitions that we must consume.
	// Add them to our using map and assign them.
	for topic, partitions := range toUse {
		topicUsing, exists := d.using[topic]
		if !exists {
			topicUsing = make(map[int32]struct{})
			d.using[topic] = topicUsing
		}
		for partition := range partitions {
			topicUsing[partition] = struct{}{}
		}
	}

	return toUse
}
