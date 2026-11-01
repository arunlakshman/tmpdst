package kafkawire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

func sampleRequest(key, v int16) kmsg.Request {
	switch key {
	case KeyProduce:
		r := kmsg.NewPtrProduceRequest()
		r.Acks, r.TimeoutMillis = -1, 1500
		b, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("v")}})
		r.Topics = []kmsg.ProduceRequestTopic{{Topic: "t", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 2, Records: b}}}}
		return r
	case KeyFetch:
		r := kmsg.NewPtrFetchRequest()
		r.MaxWaitMillis, r.MinBytes, r.MaxBytes = 100, 1, 1<<20
		r.Topics = []kmsg.FetchRequestTopic{{Topic: "t", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 1, FetchOffset: 9, PartitionMaxBytes: 4096, CurrentLeaderEpoch: -1}}}}
		return r
	case KeyListOffsets:
		r := kmsg.NewPtrListOffsetsRequest()
		r.ReplicaID = -1
		r.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: "t", Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, CurrentLeaderEpoch: -1, Timestamp: -1}}}}
		return r
	case KeyMetadata:
		r := kmsg.NewPtrMetadataRequest()
		n := "t"
		r.Topics = []kmsg.MetadataRequestTopic{{Topic: &n}}
		return r
	case KeyDescribeConfigs:
		r := kmsg.NewPtrDescribeConfigsRequest()
		r.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: 2, ResourceName: "t"}}
		return r
	case KeyCreateTopics:
		r := kmsg.NewPtrCreateTopicsRequest()
		r.TimeoutMillis, r.ValidateOnly = 5000, true
		r.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "t", NumPartitions: 3, ReplicationFactor: -1,
			Configs: []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: new("1000")}}}}
		return r
	case KeyOffsetCommit:
		r := kmsg.NewPtrOffsetCommitRequest()
		r.Group, r.Generation, r.MemberID = "g", 4, "m-1"
		r.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: "t", Partitions: []kmsg.OffsetCommitRequestTopicPartition{{Partition: 1, Offset: 7, LeaderEpoch: -1}}}}
		return r
	case KeyOffsetFetch:
		r := kmsg.NewPtrOffsetFetchRequest()
		r.Group = "g"
		r.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: "t", Partitions: []int32{0, 1}}}
		r.Groups = []kmsg.OffsetFetchRequestGroup{{Group: "g", Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: "t", Partitions: []int32{0}}}}}
		return r
	case KeyFindCoordinator:
		r := kmsg.NewPtrFindCoordinatorRequest()
		r.CoordinatorKey, r.CoordinatorType = "g", 0
		r.CoordinatorKeys = []string{"g", "h"}
		return r
	case KeyJoinGroup:
		r := kmsg.NewPtrJoinGroupRequest()
		r.Group, r.SessionTimeoutMillis, r.RebalanceTimeoutMillis, r.ProtocolType = "g", 10000, 30000, "consumer"
		r.Protocols = []kmsg.JoinGroupRequestProtocol{{Name: "range", Metadata: []byte("sub")}}
		return r
	case KeyHeartbeat:
		r := kmsg.NewPtrHeartbeatRequest()
		r.Group, r.Generation, r.MemberID = "g", 2, "m-1"
		return r
	case KeyLeaveGroup:
		r := kmsg.NewPtrLeaveGroupRequest()
		r.Group = "g"
		r.Members = []kmsg.LeaveGroupRequestMember{{MemberID: "m-1"}}
		return r
	case KeySyncGroup:
		r := kmsg.NewPtrSyncGroupRequest()
		r.Group, r.Generation, r.MemberID = "g", 2, "m-1"
		r.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{{MemberID: "m-1", MemberAssignment: []byte("a")}}
		return r
	case KeyDescribeGroups:
		r := kmsg.NewPtrDescribeGroupsRequest()
		r.Groups = []string{"g"}
		return r
	case KeyListGroups:
		r := kmsg.NewPtrListGroupsRequest()
		r.StatesFilter = []string{"Stable"}
		return r
	default: // KeyApiVersions
		r := kmsg.NewPtrApiVersionsRequest()
		if v >= 3 {
			r.ClientSoftwareName, r.ClientSoftwareVersion = "kwire", "1"
		}
		return r
	}
}

func sampleResponse(key int16, h core.RequestHeader) kmsg.Response {
	switch key {
	case KeyProduce:
		r := kmsg.NewPtrProduceResponse()
		r.Topics = []kmsg.ProduceResponseTopic{{Topic: "t", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 2, BaseOffset: 5, LogAppendTime: -1, LogStartOffset: -1, CurrentLeader: kmsg.ProduceResponseTopicPartitionCurrentLeader{LeaderID: -1, LeaderEpoch: -1}}}}}
		return r
	case KeyFetch:
		r := kmsg.NewPtrFetchResponse()
		b, _ := BuildBatch(testParams, []kmsg.Record{{Value: []byte("v")}})
		r.Topics = []kmsg.FetchResponseTopic{{Topic: "t", Partitions: []kmsg.FetchResponseTopicPartition{{Partition: 1, HighWatermark: 10, LastStableOffset: 10, LogStartOffset: 0, RecordBatches: b}}}}
		return r
	case KeyListOffsets:
		r := kmsg.NewPtrListOffsetsResponse()
		r.Topics = []kmsg.ListOffsetsResponseTopic{{Topic: "t", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Timestamp: -1, Offset: 12, LeaderEpoch: 1}}}}
		return r
	case KeyMetadata:
		r := kmsg.NewPtrMetadataResponse()
		r.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "h", Port: 9092}}
		t := "t"
		r.Topics = []kmsg.MetadataResponseTopic{{Topic: &t, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}}}
		return r
	case KeyDescribeConfigs:
		r := kmsg.NewPtrDescribeConfigsResponse()
		msg := "unknown topic"
		r.Resources = []kmsg.DescribeConfigsResponseResource{
			{ResourceType: 2, ResourceName: "t", Configs: []kmsg.DescribeConfigsResponseResourceConfig{}},
			{ResourceType: 2, ResourceName: "u", ErrorCode: 3, ErrorMessage: &msg},
		}
		return r
	case KeyCreateTopics:
		r := kmsg.NewPtrCreateTopicsResponse()
		msg := "partitions must be at least 1"
		r.Topics = []kmsg.CreateTopicsResponseTopic{
			{Topic: "t", NumPartitions: 3, ReplicationFactor: 1},
			{Topic: "u", ErrorCode: 37, ErrorMessage: &msg, NumPartitions: -1, ReplicationFactor: -1},
		}
		return r
	case KeyOffsetCommit:
		r := kmsg.NewPtrOffsetCommitResponse()
		r.Topics = []kmsg.OffsetCommitResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: 1, ErrorCode: 22}}}}
		return r
	case KeyOffsetFetch:
		r := kmsg.NewPtrOffsetFetchResponse()
		r.Topics = []kmsg.OffsetFetchResponseTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: -1, LeaderEpoch: -1, ErrorCode: 0}}}}
		r.Groups = []kmsg.OffsetFetchResponseGroup{{Group: "g", Topics: []kmsg.OffsetFetchResponseGroupTopic{{Topic: "t", Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{{Partition: 0, Offset: 5, LeaderEpoch: 0, ErrorCode: 0}}}}}}
		return r
	case KeyFindCoordinator:
		r := kmsg.NewPtrFindCoordinatorResponse()
		r.NodeID, r.Host, r.Port = 2, "h", 9092
		r.Coordinators = []kmsg.FindCoordinatorResponseCoordinator{{Key: "g", NodeID: 2, Host: "h", Port: 9092}}
		return r
	case KeyJoinGroup:
		r := kmsg.NewPtrJoinGroupResponse()
		r.Generation, r.LeaderID, r.MemberID = 3, "m-1", "m-1"
		r.Members = []kmsg.JoinGroupResponseMember{{MemberID: "m-1", ProtocolMetadata: []byte("sub")}}
		return r
	case KeyHeartbeat:
		return kmsg.NewPtrHeartbeatResponse()
	case KeyLeaveGroup:
		r := kmsg.NewPtrLeaveGroupResponse()
		r.Members = []kmsg.LeaveGroupResponseMember{{MemberID: "m-1", ErrorCode: 25}}
		return r
	case KeySyncGroup:
		r := kmsg.NewPtrSyncGroupResponse()
		r.MemberAssignment = []byte("a")
		return r
	case KeyDescribeGroups:
		r := kmsg.NewPtrDescribeGroupsResponse()
		r.Groups = []kmsg.DescribeGroupsResponseGroup{{Group: "g", State: "Stable", ProtocolType: "consumer", Protocol: "range",
			Members: []kmsg.DescribeGroupsResponseGroupMember{{MemberID: "m-1", ClientID: "c", ClientHost: "/h", ProtocolMetadata: []byte("sub"), MemberAssignment: []byte("a")}}}}
		return r
	case KeyListGroups:
		r := kmsg.NewPtrListGroupsResponse()
		r.Groups = []kmsg.ListGroupsResponseGroup{{Group: "g", ProtocolType: "consumer", GroupState: "Stable"}}
		return r
	default: // KeyApiVersions
		return ApiVersionsResponse(h)
	}
}

func oneFrame(t *testing.T, b []byte) []byte {
	t.Helper()
	fr, err := NewFramer(1 << 20).Push(b)
	if err != nil || len(fr) != 1 {
		t.Fatalf("framer: %d frames, %v", len(fr), err)
	}
	return fr[0]
}

func TestRoundTripAllAPIs(t *testing.T) {
	var c Codec
	for _, r := range Supported() {
		for _, v := range []int16{r.Min, r.Max} {
			h := core.RequestHeader{APIKey: r.Key, APIVersion: v, CorrelationID: 1000 + int32(v), ClientID: "cli"}
			req := sampleRequest(r.Key, v)
			wire, err := c.EncodeRequest(h, req)
			if err != nil {
				t.Fatal(err)
			}
			if n := binary.BigEndian.Uint32(wire); int(n) != len(wire)-4 {
				t.Fatalf("key %d v%d: bad prefix", r.Key, v)
			}
			gh, greq, err := c.DecodeRequest(oneFrame(t, wire))
			if err != nil || gh != h {
				t.Fatalf("key %d v%d: %+v %v", r.Key, v, gh, err)
			}
			if !bytes.Equal(greq.(kmsg.Request).AppendTo(nil), req.AppendTo(nil)) {
				t.Fatalf("key %d v%d: request differs\n%+v\n%+v", r.Key, v, greq, req)
			}
			resp := sampleResponse(r.Key, h)
			rw, err := c.EncodeResponse(h, resp)
			if err != nil {
				t.Fatal(err)
			}
			gresp, err := c.DecodeResponse(h, oneFrame(t, rw))
			if err != nil || !bytes.Equal(gresp.(kmsg.Response).AppendTo(nil), resp.AppendTo(nil)) {
				t.Fatalf("key %d v%d: response %v\n%+v\n%+v", r.Key, v, err, gresp, resp)
			}
			// determinism
			wire2, _ := c.EncodeRequest(h, req)
			rw2, _ := c.EncodeResponse(h, resp)
			if !bytes.Equal(wire, wire2) || !bytes.Equal(rw, rw2) {
				t.Fatalf("key %d v%d: encoding not deterministic", r.Key, v)
			}
			// correlation mismatch
			h2 := h
			h2.CorrelationID++
			if _, err := c.DecodeResponse(h2, oneFrame(t, rw)); !errors.Is(err, ErrMalformed) {
				t.Fatal("correlation mismatch accepted")
			}
		}
	}
}

func TestResponseHeaderTags(t *testing.T) {
	var c Codec
	flex := core.RequestHeader{APIKey: KeyMetadata, APIVersion: 9, CorrelationID: 7}
	rw, _ := c.EncodeResponse(flex, sampleResponse(KeyMetadata, flex))
	if rw[8] != 0 { // after prefix + corr id: header tag count
		t.Fatal("flexible response lacks header tag byte")
	}
	// ApiVersions v3 is flexible but uses response header v0: body follows
	// the correlation id directly (ThrottleMillis is absent in the error-free
	// v3 body start: error code int16 at offset 8).
	av := core.RequestHeader{APIKey: KeyApiVersions, APIVersion: 3, CorrelationID: 7}
	rw, _ = c.EncodeResponse(av, ApiVersionsResponse(av))
	if binary.BigEndian.Uint16(rw[8:]) != 0 || rw[10] == 0 && rw[11] == 0 {
		t.Fatalf("ApiVersions v3 header not v0: % x", rw[:14])
	}
}

func TestUnsupported(t *testing.T) {
	var c Codec
	hdr := func(key, v int16) []byte {
		b := binary.BigEndian.AppendUint16(nil, uint16(key))
		b = binary.BigEndian.AppendUint16(b, uint16(v))
		b = binary.BigEndian.AppendUint32(b, 42)
		return append(b, 0, 2, 'i', 'd')
	}
	for _, tc := range []struct {
		key, v  int16
		unknown bool
	}{{KeyApiVersions, 4, false}, {KeyApiVersions, 99, false}, {KeyProduce, 2, false}, {KeyFetch, 13, false}, {KeyApiVersions, -1, false}, {50, 0, true}, {-3, 0, true}} {
		h, req, err := c.DecodeRequest(append(hdr(tc.key, tc.v), 1, 2, 3))
		var ue *UnsupportedError
		if !errors.As(err, &ue) || ue.UnknownKey != tc.unknown || ue.Key != tc.key || ue.Version != tc.v || req != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		if h.CorrelationID != 42 || h.ClientID != "id" || h.APIKey != tc.key || h.APIVersion != tc.v {
			t.Fatalf("header %+v", h)
		}
	}
	// Broker answer to ApiVersions v99: error 35, encoded as v0, client decodes it.
	h := core.RequestHeader{APIKey: KeyApiVersions, APIVersion: 99, CorrelationID: 42}
	resp := ApiVersionsResponse(h)
	if resp.ErrorCode != ErrCodeUnsupportedVersion || len(resp.ApiKeys) != len(Supported()) {
		t.Fatalf("%+v", resp)
	}
	rw, err := c.EncodeResponse(h, resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.DecodeResponse(h, oneFrame(t, rw))
	if err != nil {
		t.Fatal(err)
	}
	g := got.(*kmsg.ApiVersionsResponse)
	if g.Version != 0 || g.ErrorCode != 35 || len(g.ApiKeys) != len(Supported()) {
		t.Fatalf("%+v", g)
	}
}

func TestMalformed(t *testing.T) {
	var c Codec
	for _, f := range [][]byte{nil, {0, 3}, make([]byte, 9), {0, 3, 0, 1, 0, 0, 0, 1, 0xff, 0xfe}, {0, 3, 0, 1, 0, 0, 0, 1, 0, 5, 'a'}} {
		if _, _, err := c.DecodeRequest(f); err == nil {
			t.Fatalf("%x accepted", f)
		}
	}
	h := core.RequestHeader{APIKey: KeyMetadata, APIVersion: 1, CorrelationID: 1}
	if _, err := c.EncodeResponse(h, kmsg.NewPtrFetchResponse()); err == nil {
		t.Fatal("mismatched response accepted")
	}
	if _, err := c.EncodeRequest(h, kmsg.NewPtrFetchRequest()); err == nil {
		t.Fatal("mismatched request accepted")
	}
}

func FuzzDecodeRequest(f *testing.F) {
	var c Codec
	for _, r := range Supported() {
		h := core.RequestHeader{APIKey: r.Key, APIVersion: r.Max, CorrelationID: 3, ClientID: "c"}
		w, _ := c.EncodeRequest(h, sampleRequest(r.Key, r.Max))
		f.Add(w[4:])
		w, _ = c.EncodeResponse(h, sampleResponse(r.Key, h))
		f.Add(w[4:])
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, req, err := c.DecodeRequest(data); err == nil {
			_, _ = c.EncodeRequest(core.RequestHeader{APIKey: req.Key(), APIVersion: req.GetVersion()}, req)
		}
		for _, r := range Supported() {
			_, _ = c.DecodeResponse(core.RequestHeader{APIKey: r.Key, APIVersion: r.Max}, data)
		}
	})
}

// InitProducerId (key 22) is not served (decision 41): it is neither
// advertised nor decodable, so idempotent clients fail at startup.
func TestInitProducerIDNotSupported(t *testing.T) {
	if IsSupported(22, 0) {
		t.Fatal("key 22 is supported")
	}
	for _, r := range ApiVersionsResponse(core.RequestHeader{APIKey: KeyApiVersions, APIVersion: 3}).ApiKeys {
		if r.ApiKey == 22 {
			t.Fatal("ApiVersions advertises InitProducerId")
		}
	}
	var c Codec
	req := kmsg.NewPtrInitProducerIDRequest()
	wire, err := c.EncodeRequest(core.RequestHeader{APIKey: 22, APIVersion: 0, CorrelationID: 1}, req)
	if err == nil {
		if _, _, err = c.DecodeRequest(oneFrame(t, wire)); err == nil {
			t.Fatal("key 22 decoded")
		}
	}
}
