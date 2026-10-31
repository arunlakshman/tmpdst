package broker

import (
	"math"
	"slices"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// DescribeGroups (key 15) and ListGroups (key 16). Admin tools read them. Each
// broker lists only the groups it coordinates, so the admin client, which asks
// every broker and merges the answers, sees each group once.

// authOpsOmitted is what Kafka answers for AuthorizedOperations when the
// client did not ask for them (version 3 and up).
const authOpsOmitted = math.MinInt32

// describeGroups answers DescribeGroups. Every name gets an entry, in request
// order. A group the control plane does not know is "Dead" with no error.
func (b *Broker) describeGroups(s *slot, r *kmsg.DescribeGroupsRequest) []core.Action {
	response := kmsg.NewPtrDescribeGroupsResponse()
	s.response = response
	if len(r.Groups) == 0 {
		return nil
	}
	response.Groups = make([]kmsg.DescribeGroupsResponseGroup, len(r.Groups))
	for i, name := range r.Groups {
		response.Groups[i].Group = name
		response.Groups[i].AuthorizedOperations = authOpsOmitted
	}
	fill := func(e core.ControlPlaneDone) {
		ds := core.ResponseAs[core.DescribeGroupsResp](e).Groups
		for i := range response.Groups {
			g := &response.Groups[i]
			switch {
			case e.Err != core.OK:
				g.ErrorCode = int16(groupErrCode(e.Err))
			case i < len(ds):
				describedGroup(g, ds[i])
			}
		}
	}
	return b.groupCall(s, core.DescribeGroups{Groups: r.Groups}, 0, fill)
}

// describedGroup writes one group of a DescribeGroups answer.
func describedGroup(g *kmsg.DescribeGroupsResponseGroup, d core.GroupDescription) {
	if !d.Exists {
		g.State = "Dead"
		return
	}
	g.State, g.ProtocolType, g.Protocol = kafkaGroupState(d.State), d.ProtocolType, d.Protocol
	for _, m := range d.Members {
		g.Members = append(g.Members, kmsg.DescribeGroupsResponseGroupMember{
			MemberID: m.MemberID, InstanceID: optStr(m.GroupInstanceID), ClientID: m.ClientID,
			ClientHost: m.ClientHost, ProtocolMetadata: m.Metadata, MemberAssignment: m.Assignment,
		})
	}
}

// kafkaGroupState is the Kafka name of a control plane group state. The control
// plane's PreparingSync is Kafka's CompletingRebalance.
func kafkaGroupState(s string) string {
	if s == "PreparingSync" {
		return "CompletingRebalance"
	}
	return s
}

// inStates reports whether state is in filter. An empty filter takes every state.
func inStates(filter []string, state string) bool {
	return len(filter) == 0 || slices.Contains(filter, state)
}

// listGroups answers ListGroups. It lists the groups this broker coordinates
// (FindCoordinator hash), and a states filter (version 4 and up) narrows them.
// With no other live broker, this broker coordinates every group.
func (b *Broker) listGroups(s *slot, r *kmsg.ListGroupsRequest) []core.Action {
	response := kmsg.NewPtrListGroupsResponse()
	s.response = response
	fill := func(e core.ControlPlaneDone) {
		response.ErrorCode = int16(groupErrCode(e.Err))
		if e.Err != core.OK {
			return
		}
		for _, g := range core.ResponseAs[core.ListGroupsResp](e).Groups {
			state := kafkaGroupState(g.State)
			if !b.coordinatorIsSelf(g.Name) || !inStates(r.StatesFilter, state) {
				continue
			}
			response.Groups = append(response.Groups, kmsg.ListGroupsResponseGroup{Group: g.Name, ProtocolType: g.ProtocolType, GroupState: state})
		}
	}
	return b.groupCall(s, core.ListGroups{}, 0, fill)
}
