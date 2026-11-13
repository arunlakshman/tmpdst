package client

import (
	"context"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/contract"
)

// callTopic sends one topics operation.
func (c *Client) callTopic(ctx context.Context, operation contract.Operation) (contract.Response, error) {
	switch operation := operation.(type) {
	case contract.CommitSegment:
		return c.commitSegment(ctx, operation)
	case contract.FindSegments:
		return c.findSegments(ctx, operation)
	case contract.DescribeTopics:
		return c.describeTopics(ctx, operation)
	case contract.CreateTopic:
		return nil, c.createTopic(ctx, operation)
	}
	return nil, &Error{Code: contract.ErrInvalid}
}

func (c *Client) commitSegment(ctx context.Context, operation contract.CommitSegment) (contract.Response, error) {
	return callOp(ctx, c, "CommitSegment", cpv1.ControlPlaneClient.CommitSegment,
		commitSegmentFromContract(operation), commitSegmentRespToContract)
}

func (c *Client) findSegments(ctx context.Context, operation contract.FindSegments) (contract.Response, error) {
	return callOp(ctx, c, "FindSegments", cpv1.ControlPlaneClient.FindSegments,
		findSegmentsFromContract(operation), findSegmentsRespToContract)
}

func (c *Client) describeTopics(ctx context.Context, operation contract.DescribeTopics) (contract.Response, error) {
	return callOp(ctx, c, "DescribeTopics", cpv1.ControlPlaneClient.DescribeTopics,
		describeTopicsFromContract(operation), describeTopicsRespToContract)
}

// CreateTopic is the dev/test helper over CreateTopic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	return c.createTopic(ctx, contract.CreateTopic{Name: name, Partitions: partitions})
}

// createTopic sends op. FailIfExists makes the create exclusive.
func (c *Client) createTopic(ctx context.Context, operation contract.CreateTopic) error {
	return c.do(ctx, "CreateTopic", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{
			Topic: operation.Name, Partitions: operation.Partitions, FailIfExists: operation.FailIfExists,
		})
		return err
	})
}
