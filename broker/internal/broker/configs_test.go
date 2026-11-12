package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const verDescribeConfigs = 4

func TestDescribeConfigsTopics(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	q := kmsg.NewPtrDescribeConfigsRequest()
	q.Resources = []kmsg.DescribeConfigsRequestResource{
		{ResourceType: resourceTopic, ResourceName: "t"},
		{ResourceType: resourceTopic, ResourceName: "nope"},
		{ResourceType: kmsg.ConfigResourceType(4), ResourceName: "1"},
	}
	r := h.call(c, verDescribeConfigs, q).(*kmsg.DescribeConfigsResponse)
	if len(r.Resources) != 3 {
		t.Fatalf("%d resources, want 3", len(r.Resources))
	}
	if rr := r.Resources[0]; rr.ErrorCode != 0 || len(rr.Configs) != 0 || rr.ResourceName != "t" {
		t.Errorf("existing topic: %+v", rr)
	}
	if rr := r.Resources[1]; kafkaErrorCode(rr.ErrorCode) != errUnknownTopicOrPart {
		t.Errorf("unknown topic: %+v", rr)
	}
	if rr := r.Resources[2]; kafkaErrorCode(rr.ErrorCode) != errInvalidRequest {
		t.Errorf("broker resource: %+v", rr)
	}
	h.noViolations()
}

func TestDescribeConfigsNoTopicsAsksNothing(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	q := kmsg.NewPtrDescribeConfigsRequest()
	q.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: kmsg.ConfigResourceType(4), ResourceName: "1"}}
	r := h.call(c, verDescribeConfigs, q).(*kmsg.DescribeConfigsResponse)
	if kafkaErrorCode(r.Resources[0].ErrorCode) != errInvalidRequest {
		t.Fatalf("%+v", r.Resources[0])
	}
	if len(h.cpCalls) != 0 {
		t.Fatalf("control plane called %d times for no topics", len(h.cpCalls))
	}
	h.noViolations()
}
