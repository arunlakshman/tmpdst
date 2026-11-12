package cpclient

import (
	"context"

	"github.com/manifesterio/mds/broker/internal/core"
	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
)

// callTopic sends one topics operation.
func (c *Client) callTopic(ctx context.Context, operation core.ControlPlaneOperation) (core.ControlPlaneResponse, error) {
	switch operation := operation.(type) {
	case core.CommitSegment:
		return c.commitSegment(ctx, operation)
	case core.FindSegments:
		return c.findSegments(ctx, operation)
	case core.DescribeTopics:
		return c.describeTopics(ctx, operation)
	case core.CreateTopic:
		return nil, c.createTopic(ctx, operation)
	}
	return nil, &Error{Code: core.ErrInvalid}
}

func (c *Client) commitSegment(ctx context.Context, operation core.CommitSegment) (core.ControlPlaneResponse, error) {
	return callOp(ctx, c, "CommitSegment", cpv1.ControlPlaneClient.CommitSegment,
		commitSegmentFromCore(operation), commitSegmentRespToCore)
}

func (c *Client) findSegments(ctx context.Context, operation core.FindSegments) (core.ControlPlaneResponse, error) {
	return callOp(ctx, c, "FindSegments", cpv1.ControlPlaneClient.FindSegments,
		findSegmentsFromCore(operation), findSegmentsRespToCore)
}

func (c *Client) describeTopics(ctx context.Context, operation core.DescribeTopics) (core.ControlPlaneResponse, error) {
	return callOp(ctx, c, "DescribeTopics", cpv1.ControlPlaneClient.DescribeTopics,
		describeTopicsFromCore(operation), describeTopicsRespToCore)
}

// CreateTopic is the dev/test helper over CreateTopic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	return c.createTopic(ctx, core.CreateTopic{Name: name, Partitions: partitions})
}

// createTopic sends op. FailIfExists makes the create exclusive.
func (c *Client) createTopic(ctx context.Context, operation core.CreateTopic) error {
	return c.do(ctx, "CreateTopic", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{
			Topic: operation.Name, Partitions: operation.Partitions, FailIfExists: operation.FailIfExists,
		})
		return err
	})
}
