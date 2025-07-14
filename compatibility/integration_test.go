//go:build integration
// +build integration

package compatibility

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Test workflows for integration testing
func oldIntegrationWorkflow(ctx workflow.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

func newIntegrationWorkflow(ctx workflow.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

func compatibleIntegrationWorkflow(ctx workflow.Context, name string, args ...interface{}) (string, error) {
	return WorkflowAdapter(ctx, "integration-test-change",
		func() (string, error) {
			return oldIntegrationWorkflow(ctx, name)
		},
		func() (string, error) {
			if len(args) == 0 {
				return "", errors.New("missing greeting parameter")
			}
			greeting, ok := args[0].(string)
			if !ok {
				return "", errors.New("greeting must be a string")
			}
			return newIntegrationWorkflow(ctx, name, greeting)
		})
}

// Test activities for integration testing
func oldIntegrationActivity(ctx context.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

func newIntegrationActivity(ctx context.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

func compatibleIntegrationActivity(ctx context.Context, name string, args ...interface{}) (string, error) {
	return ActivityAdapter(ctx, "integration-test-change",
		func() (string, error) {
			return oldIntegrationActivity(ctx, name)
		},
		func() (string, error) {
			if len(args) == 0 {
				return "", errors.New("missing greeting parameter")
			}
			greeting, ok := args[0].(string)
			if !ok {
				return "", errors.New("greeting must be a string")
			}
			return newIntegrationActivity(ctx, name, greeting)
		})
}

// Workflow that executes activities with versioning
func workflowWithVersionedActivity(ctx workflow.Context, name string, useNewVersion bool, args ...interface{}) (string, error) {
	// Determine which version to use for activities
	var activityCtx workflow.Context
	if useNewVersion {
		activityCtx = EnsureVersion(ctx, "integration-test-change", 1)
	} else {
		activityCtx = ctx
	}
	
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	}
	activityCtx = workflow.WithActivityOptions(activityCtx, activityOptions)
	
	// Execute activity
	var result string
	var err error
	if len(args) > 0 {
		err = workflow.ExecuteActivity(activityCtx, compatibleIntegrationActivity, name, args[0]).Get(activityCtx, &result)
	} else {
		err = workflow.ExecuteActivity(activityCtx, compatibleIntegrationActivity, name).Get(activityCtx, &result)
	}
	if err != nil {
		return "", err
	}
	
	return result, nil
}

// Integration test for workflow signature changes
func TestIntegrationWorkflowSignatureChanges(t *testing.T) {
	// Create a client
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233", // Default Temporal server address
	})
	if err != nil {
		t.Skip("Skipping integration test, Temporal server not available:", err)
		return
	}
	defer c.Close()
	
	// Create a unique task queue for this test
	taskQueue := "integration-test-" + uuid.New().String()
	
	// Create a worker
	w := worker.New(c, taskQueue, worker.Options{})
	
	// Register workflows and activities
	w.RegisterWorkflow(compatibleIntegrationWorkflow)
	
	// Start the worker
	err = w.Start()
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()
	
	// Test with old signature
	workflowOptions := client.StartWorkflowOptions{
		ID:        "integration-test-old-signature-" + uuid.New().String(),
		TaskQueue: taskQueue,
	}
	
	run, err := c.ExecuteWorkflow(context.Background(), workflowOptions, compatibleIntegrationWorkflow, "World")
	require.NoError(t, err)
	
	var result string
	err = run.Get(context.Background(), &result)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World", result)
	
	// Test with new signature
	workflowOptions = client.StartWorkflowOptions{
		ID:        "integration-test-new-signature-" + uuid.New().String(),
		TaskQueue: taskQueue,
	}
	
	run, err = c.ExecuteWorkflow(context.Background(), workflowOptions, compatibleIntegrationWorkflow, "World", "Greetings")
	require.NoError(t, err)
	
	err = run.Get(context.Background(), &result)
	require.NoError(t, err)
	assert.Equal(t, "Greetings, World", result)
}

// Integration test for activity signature changes
func TestIntegrationActivitySignatureChanges(t *testing.T) {
	// Create a client
	c, err := client.Dial(client.Options{})
	if err != nil {
		t.Skip("Skipping integration test, Temporal server not available:", err)
		return
	}
	defer c.Close()
	
	// Create a unique task queue for this test
	taskQueue := "integration-test-" + uuid.New().String()
	
	// Create a worker
	w := worker.New(c, taskQueue, worker.Options{})
	
	// Register the context propagator
	RegisterWithWorker(w)
	
	// Register workflows and activities
	w.RegisterWorkflow(workflowWithVersionedActivity)
	w.RegisterActivity(compatibleIntegrationActivity)
	
	// Start the worker
	err = w.Start()
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer w.Stop()
	
	// Test with old signature
	workflowOptions := client.StartWorkflowOptions{
		ID:        "integration-test-old-activity-" + uuid.New().String(),
		TaskQueue: taskQueue,
	}
	
	run, err := c.ExecuteWorkflow(context.Background(), workflowOptions, workflowWithVersionedActivity, "World", false)
	require.NoError(t, err)
	
	var result string
	err = run.Get(context.Background(), &result)
	require.NoError(t, err)
	assert.Equal(t, "Hello, World", result)
	
	// Test with new signature
	workflowOptions = client.StartWorkflowOptions{
		ID:        "integration-test-new-activity-" + uuid.New().String(),
		TaskQueue: taskQueue,
	}
	
	run, err = c.ExecuteWorkflow(context.Background(), workflowOptions, workflowWithVersionedActivity, "World", true, "Greetings")
	require.NoError(t, err)
	
	err = run.Get(context.Background(), &result)
	require.NoError(t, err)
	assert.Equal(t, "Greetings, World", result)
}
