package compatibility_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/compatibility"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Old workflow signature
func OldWorkflow(ctx workflow.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

// New workflow signature with an additional parameter
func NewWorkflow(ctx workflow.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

// Combined workflow that supports both signatures
func ExampleWorkflowAdapter() {
	// This is the workflow function that will be registered with Temporal
	workflowFn := func(ctx workflow.Context, name string, args ...interface{}) (string, error) {
		return compatibility.WorkflowAdapter(ctx, "add-greeting-param",
			func() (string, error) {
				// Call the old workflow implementation
				return OldWorkflow(ctx, name)
			},
			func() (string, error) {
				// Call the new workflow implementation with additional parameters
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return NewWorkflow(ctx, name, greeting)
			})
	}

	// Register the workflow with a worker
	w := worker.New(client.Client{}, "task-queue", worker.Options{})
	w.RegisterWorkflow(workflowFn)

	// The workflow can now be called with either the old or new signature:
	// Old: client.ExecuteWorkflow(ctx, options, workflowFn, "World")
	// New: client.ExecuteWorkflow(ctx, options, workflowFn, "World", "Greetings")
}

// Example showing how to use WorkflowAdapterFunc for a simpler implementation
func ExampleWorkflowAdapterFunc() {
	// Create an adapter directly from the two workflow functions
	adaptedWorkflow := compatibility.WorkflowAdapterFunc(
		"add-greeting-param",
		OldWorkflow,
		NewWorkflow,
	)

	// Register the adapter with the worker
	w := worker.New(client.Client{}, "task-queue", worker.Options{})
	w.RegisterWorkflow(adaptedWorkflow)

	// The workflow can now be called with either the old or new signature:
	// Old: client.ExecuteWorkflow(ctx, options, adaptedWorkflow, "World")
	// New: client.ExecuteWorkflow(ctx, options, adaptedWorkflow, "World", "Greetings")
}

// Old activity signature
func OldActivity(ctx context.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

// New activity signature with an additional parameter
func NewActivity(ctx context.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

// Combined activity that supports both signatures
func ExampleActivityAdapter() {
	// This is the activity function that will be registered with Temporal
	activityFn := func(ctx context.Context, name string, args ...interface{}) (string, error) {
		return compatibility.ActivityAdapter(ctx, "add-greeting-param",
			func() (string, error) {
				// Call the old activity implementation
				return OldActivity(ctx, name)
			},
			func() (string, error) {
				// Call the new activity implementation with additional parameters
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return NewActivity(ctx, name, greeting)
			})
	}

	// Register the activity with a worker
	w := worker.New(client.Client{}, "task-queue", worker.Options{})
	w.RegisterActivity(activityFn)

	// Register the context propagator to ensure version information is passed
	compatibility.RegisterWithWorker(w)
}

// Example showing a complete workflow that uses versioned activities
func Example_completeExample() {
	// This workflow demonstrates how to call activities with different versions
	versionedWorkflow := func(ctx workflow.Context, name string) (string, error) {
		// Determine which version of the workflow we're running
		version := workflow.GetVersion(ctx, "workflow-signature-change", workflow.DefaultVersion, 1)
		
		// Set up activity options
		activityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		}
		activityCtx := workflow.WithActivityOptions(ctx, activityOptions)
		
		// Ensure activities use the same version as the workflow
		activityCtx = compatibility.EnsureVersion(activityCtx, "activity-signature-change", version)
		
		// Call activity with parameters based on the version
		var result string
		var err error
		
		if version == workflow.DefaultVersion {
			// Old version
			err = workflow.ExecuteActivity(activityCtx, "VersionedActivity", name).Get(activityCtx, &result)
		} else {
			// New version
			err = workflow.ExecuteActivity(activityCtx, "VersionedActivity", name, "Greetings").Get(activityCtx, &result)
		}
		
		if err != nil {
			return "", err
		}
		
		return fmt.Sprintf("Workflow result: %s", result), nil
	}
	
	// The versioned activity implementation
	versionedActivity := func(ctx context.Context, name string, args ...interface{}) (string, error) {
		return compatibility.ActivityAdapter(ctx, "activity-signature-change",
			func() (string, error) {
				return OldActivity(ctx, name)
			},
			func() (string, error) {
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return NewActivity(ctx, name, greeting)
			})
	}
	
	// Set up the worker
	c, _ := client.Dial(client.Options{})
	w := worker.New(c, "task-queue", worker.Options{})
	
	// Register workflow and activity
	w.RegisterWorkflow(versionedWorkflow)
	w.RegisterActivity(versionedActivity, worker.RegisterActivityOptions{Name: "VersionedActivity"})
	
	// Register the context propagator
	compatibility.RegisterWithWorker(w)
	
	// Start the worker
	_ = w.Start()
	
	// Execute the workflow
	workflowOptions := client.StartWorkflowOptions{
		ID:        "example-workflow",
		TaskQueue: "task-queue",
	}
	
	run, _ := c.ExecuteWorkflow(context.Background(), workflowOptions, versionedWorkflow, "World")
	
	// Wait for the workflow to complete
	var result string
	_ = run.Get(context.Background(), &result)
	fmt.Println(result)
}
