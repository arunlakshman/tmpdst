package broker

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// createTopics answers CreateTopics (Kafka key 19, decision Q10). Each topic is
// checked here first. A topic that passes needs the control plane: one
// exclusive CreateTopic per topic. The control plane checks that the topic is
// new and creates it in one step, so two creates of one new name cannot both
// succeed. validateOnly only reads, so it asks DescribeTopics and creates
// nothing.
//
// Replication is ignored: object storage is the replication. Configs are
// ignored, and one warning fact is logged per request that has any. Topic ids
// are not kept, so v7 answers the zero id.
func (b *Broker) createTopics(s *slot, r *kmsg.CreateTopicsRequest) []core.Action {
	response := kmsg.NewPtrCreateTopicsResponse()
	response.Topics = make([]kmsg.CreateTopicsResponseTopic, len(r.Topics))
	s.response = response
	var acts []core.Action
	withConfigs := 0
	var names []string
	for i, t := range r.Topics {
		rt := &response.Topics[i]
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = t.Topic, -1, -1
		rt.Configs = []kmsg.CreateTopicsResponseTopicConfig{} // v5+ clients need a list, not null
		if len(t.Configs) > 0 {
			withConfigs++
		}
		dup := slices.Contains(names, t.Topic)
		names = append(names, t.Topic)
		if code, msg := checkCreate(t, dup); code != errNone {
			setTopicError(rt, code, msg)
			continue
		}
		o := &operation{kind: opCreate, s: s, ctopic: rt, validateOnly: r.ValidateOnly,
			create: core.CreateTopic{Name: t.Topic, Partitions: partitionsOf(t), FailIfExists: !r.ValidateOnly}}
		acts = append(acts, b.startOp(o)...)
	}
	if withConfigs > 0 {
		s.facts = append(s.facts, core.Fact{Name: "create_topics_configs_ignored", Fields: []core.Field{
			{Key: "topics", Value: core.Int64Value(withConfigs)},
		}})
	}
	return acts
}

// checkCreate is the local check of one topic. dup is true when the request
// names the topic before. It returns errNone when the topic may be created.
func checkCreate(t kmsg.CreateTopicsRequestTopic, dup bool) (kafkaErrorCode, string) {
	switch {
	case !validTopicName(t.Topic):
		return errInvalidTopic, "invalid topic name"
	case dup:
		return errInvalidRequest, "topic is in the request more than once"
	case len(t.ReplicaAssignment) > 0:
		return errInvalidRequest, "manual replica assignment is not supported"
	case t.NumPartitions != -1 && t.NumPartitions < 1:
		return errInvalidPartitions, "partitions must be at least 1"
	}
	return errNone, ""
}

// partitionsOf is the partition count of t. -1 means the broker default, which
// is one partition.
func partitionsOf(t kmsg.CreateTopicsRequestTopic) int32 {
	if t.NumPartitions == -1 {
		return 1
	}
	return t.NumPartitions
}

// validTopicName reports whether name is a Kafka topic name: 1 to 249 ASCII
// letters, digits, '.', '_' or '-', and not "." or "..".
func validTopicName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 249 {
		return false
	}
	for i := range len(name) {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		if !ok {
			return false
		}
	}
	return true
}

func setTopicError(rt *kmsg.CreateTopicsResponseTopic, code kafkaErrorCode, msg string) {
	rt.ErrorCode = int16(code)
	rt.ErrorMessage = &msg
}

// createCP is the control plane call of a create op. A real create is one
// exclusive CreateTopic. validateOnly only reads, so it asks DescribeTopics.
func (o *operation) createCP() core.ControlPlaneOperation {
	if o.validateOnly {
		return core.DescribeTopics{Topics: []string{o.create.Name}}
	}
	return o.create
}

// onCreateCP handles the answer of a create op's control plane call.
// Infrastructure errors are retried by retryOp, like the other ops.
func (b *Broker) onCreateCP(o *operation, e core.ControlPlaneDone) []core.Action {
	switch {
	case e.Err == core.ErrInvalid:
		return b.failOp(o, errInvalidRequest)
	case e.Err == core.ErrTopicExists:
		// A retry after a lost reply can find the topic that the first try
		// made. Then the topic is ours, so the create succeeded.
		if o.maybeApplied {
			return b.createdOK(o)
		}
		setTopicError(o.ctopic, errTopicExists, "topic already exists")
		return b.endOp(o)
	case e.Err != core.OK:
		if e.Outcome == core.MaybeApplied {
			o.maybeApplied = true
		}
		return b.retryOp(o)
	case o.validateOnly:
		for _, t := range core.ResponseAs[core.DescribeTopicsResp](e).Topics {
			if t.Name == o.create.Name {
				setTopicError(o.ctopic, errTopicExists, "topic already exists")
				return b.endOp(o)
			}
		}
	}
	return b.createdOK(o)
}

// createdOK ends a create op that passed. The counts are the ones the topic has.
func (b *Broker) createdOK(o *operation) []core.Action {
	o.ctopic.NumPartitions, o.ctopic.ReplicationFactor = o.create.Partitions, 1
	return b.endOp(o)
}
