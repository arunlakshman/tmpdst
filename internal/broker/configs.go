package broker

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

// resourceTopic is Kafka's config resource type for topics.
const resourceTopic = kmsg.ConfigResourceType(2)

// describeConfigs answers DescribeConfigs (Kafka key 32) for topics, which
// kafka-topics.sh --describe needs. The broker keeps no topic configs, so an
// existing topic gets an empty config list. Other resource types are refused.
func (b *Broker) describeConfigs(s *slot, r *kmsg.DescribeConfigsRequest) []core.Action {
	resp := kmsg.NewPtrDescribeConfigsResponse()
	resp.Resources = make([]kmsg.DescribeConfigsResponseResource, len(r.Resources))
	var names []string
	for i, res := range r.Resources {
		rr := &resp.Resources[i]
		rr.ResourceType, rr.ResourceName = res.ResourceType, res.ResourceName
		if res.ResourceType != resourceTopic {
			setResourceError(rr, errInvalidRequest, "only topic configs are served")
			continue
		}
		names = append(names, res.ResourceName)
	}
	s.resp = resp
	if len(names) == 0 { // nothing to ask: DescribeTopics with no names would list every topic
		return nil
	}
	return b.startOp(&operation{kind: opDescribeConfigs, s: s, names: names, dresp: resp})
}

// onDescribeConfigsCP fills the answer of a DescribeConfigs op from the topics the
// control plane knows. Resources refused earlier keep their error.
func (b *Broker) onDescribeConfigsCP(o *operation, infos []core.TopicInfo) []core.Action {
	for i := range o.dresp.Resources {
		rr := &o.dresp.Resources[i]
		if rr.ErrorCode != 0 {
			continue
		}
		if !hasTopic(infos, rr.ResourceName) {
			setResourceError(rr, errUnknownTopicOrPart, "unknown topic")
			continue
		}
		rr.Configs = []kmsg.DescribeConfigsResponseResourceConfig{}
	}
	return b.endOp(o)
}

// failDescribeConfigs gives every resource of o that has no error the code.
func (o *operation) failDescribeConfigs(code kafkaErrorCode) {
	for i := range o.dresp.Resources {
		if rr := &o.dresp.Resources[i]; rr.ErrorCode == 0 {
			setResourceError(rr, code, "")
		}
	}
}

func setResourceError(rr *kmsg.DescribeConfigsResponseResource, code kafkaErrorCode, msg string) {
	rr.ErrorCode = int16(code)
	if msg != "" {
		rr.ErrorMessage = &msg
	}
}

func hasTopic(infos []core.TopicInfo, name string) bool {
	for _, in := range infos {
		if in.Name == name {
			return true
		}
	}
	return false
}
