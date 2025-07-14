package compatibility

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Test activities
func oldActivity(ctx context.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

func newActivity(ctx context.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

func oldActivityWithError(ctx context.Context, name string) (string, error) {
	if name == "error" {
		return "", errors.New("error requested")
	}
	return "Hello, " + name, nil
}

func newActivityWithError(ctx context.Context, name string, greeting string) (string, error) {
	if greeting == "error" {
		return "", errors.New("error requested")
	}
	return greeting + ", " + name, nil
}

// Test workflow that uses activities with the adapter
func testWorkflowWithActivity(ctx workflow.Context, name string, useNewVersion bool, args ...interface{}) (string, error) {
	// Create context with version information
	var activityCtx workflow.Context
	if useNewVersion {
		activityCtx = EnsureVersion(ctx, "add-greeting-param", 1)
	} else {
		activityCtx = ctx
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
	}
	activityCtx = workflow.WithActivityOptions(activityCtx, activityOptions)

	// Execute activity
	var result string
	var err error
	if len(args) > 0 {
		err = workflow.ExecuteActivity(activityCtx, testActivity, name, args[0]).Get(activityCtx, &result)
	} else {
		err = workflow.ExecuteActivity(activityCtx, testActivity, name).Get(activityCtx, &result)
	}
	if err != nil {
		return "", err
	}

	return result, nil
}

// Test activity that uses ActivityAdapter
func testActivity(ctx context.Context, name string, args ...interface{}) (string, error) {
	return ActivityAdapter(ctx, "add-greeting-param",
		func() (string, error) {
			return oldActivity(ctx, name)
		},
		func() (string, error) {
			if len(args) == 0 {
				return "", errors.New("missing greeting parameter")
			}
			greeting, ok := args[0].(string)
			if !ok {
				return "", errors.New("greeting must be a string")
			}
			return newActivity(ctx, name, greeting)
		})
}

// Test the ActivityAdapter function
func TestActivityAdapter(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register workflow and activity
	env.RegisterWorkflow(testWorkflowWithActivity)
	env.RegisterActivity(testActivity)

	// Test with old version
	env.ExecuteWorkflow(testWorkflowWithActivity, "World", false)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflowWithActivity)
	env.RegisterActivity(testActivity)

	// Test with new version
	env.ExecuteWorkflow(testWorkflowWithActivity, "World", true, "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)

	// Test error handling
	env = testSuite.NewTestWorkflowEnvironment()

	testErrorActivity := func(ctx context.Context, name string, args ...interface{}) (string, error) {
		return ActivityAdapter(ctx, "add-greeting-param",
			func() (string, error) {
				return oldActivityWithError(ctx, name)
			},
			func() (string, error) {
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return newActivityWithError(ctx, name, greeting)
			})
	}

	testErrorWorkflow := func(ctx workflow.Context, name string, useNewVersion bool, args ...interface{}) (string, error) {
		var activityCtx workflow.Context
		if useNewVersion {
			activityCtx = EnsureVersion(ctx, "add-greeting-param", 1)
		} else {
			activityCtx = ctx
		}

		activityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 10,
		}
		activityCtx = workflow.WithActivityOptions(activityCtx, activityOptions)

		var result string
		var err error
		if len(args) > 0 {
			err = workflow.ExecuteActivity(activityCtx, testErrorActivity, name, args[0]).Get(activityCtx, &result)
		} else {
			err = workflow.ExecuteActivity(activityCtx, testErrorActivity, name).Get(activityCtx, &result)
		}
		if err != nil {
			return "", err
		}

		return result, nil
	}

	env.RegisterWorkflow(testErrorWorkflow)
	env.RegisterActivity(testErrorActivity)

	// Test with old version and error
	env.ExecuteWorkflow(testErrorWorkflow, "error", false)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "error requested")
}

// Test the ActivityAdapterFunc function
func TestActivityAdapterFunc(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Create an adapter activity
	adaptedActivity := ActivityAdapterFunc(
		"add-greeting-param",
		oldActivity,
		newActivity,
	)

	// Test workflow that uses the adapted activity
	testWorkflowWithAdaptedActivity := func(ctx workflow.Context, name string, useNewVersion bool, args ...interface{}) (string, error) {
		var activityCtx workflow.Context
		if useNewVersion {
			activityCtx = EnsureVersion(ctx, "add-greeting-param", 1)
		} else {
			activityCtx = ctx
		}

		activityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 10,
		}
		activityCtx = workflow.WithActivityOptions(activityCtx, activityOptions)

		var result string
		var err error
		if len(args) > 0 {
			err = workflow.ExecuteActivity(activityCtx, adaptedActivity, name, args[0]).Get(activityCtx, &result)
		} else {
			err = workflow.ExecuteActivity(activityCtx, adaptedActivity, name).Get(activityCtx, &result)
		}
		if err != nil {
			return "", err
		}

		return result, nil
	}

	env.RegisterWorkflow(testWorkflowWithAdaptedActivity)
	env.RegisterActivity(adaptedActivity)

	// Test with old version
	env.ExecuteWorkflow(testWorkflowWithAdaptedActivity, "World", false)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflowWithAdaptedActivity)
	env.RegisterActivity(adaptedActivity)

	// Test with new version
	env.ExecuteWorkflow(testWorkflowWithAdaptedActivity, "World", true, "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)
}

// Test the context propagator
func TestWorkflowContextPropagator(t *testing.T) {
	propagator := &WorkflowContextPropagator{}

	// Test Inject
	ctx := WithActivityVersion(context.Background(), "test-change", 1)
	headerMap := make(map[string][]byte)
	writer := &testHeaderWriter{headerMap: headerMap}

	err := propagator.Inject(ctx, writer)
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), headerMap["activity-version-test-change"])

	// Test Extract
	reader := &testHeaderReader{headerMap: headerMap}
	newCtx, err := propagator.Extract(context.Background(), reader)
	require.NoError(t, err)

	version := GetActivityVersion(newCtx, "test-change", 0)
	assert.Equal(t, 1, version)
}

// Test the WithActivityVersion and GetActivityVersion functions
func TestActivityVersionContext(t *testing.T) {
	ctx := context.Background()

	// Test default version
	version := GetActivityVersion(ctx, "test-change", 0)
	assert.Equal(t, 0, version)

	// Test setting version
	ctx = WithActivityVersion(ctx, "test-change", 1)
	version = GetActivityVersion(ctx, "test-change", 0)
	assert.Equal(t, 1, version)

	// Test multiple changes
	ctx = WithActivityVersion(ctx, "another-change", 2)
	version = GetActivityVersion(ctx, "another-change", 0)
	assert.Equal(t, 2, version)

	// Make sure the first change is still there
	version = GetActivityVersion(ctx, "test-change", 0)
	assert.Equal(t, 1, version)
}

// Test headers for workflow/activity communication
func TestEnsureVersion(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	testWorkflow := func(ctx workflow.Context, changeID string, version int) (int, error) {
		activityCtx := EnsureVersion(ctx, changeID, version)

		activityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		activityCtx = workflow.WithActivityOptions(activityCtx, activityOptions)

		var result int
		err := workflow.ExecuteActivity(activityCtx, testVersionActivity, changeID).Get(activityCtx, &result)
		if err != nil {
			return 0, err
		}

		return result, nil
	}

	testVersionActivity := func(ctx context.Context, changeID string) (int, error) {
		version := GetActivityVersion(ctx, changeID, 0)
		return version, nil
	}

	env.RegisterWorkflow(testWorkflow)
	env.RegisterActivity(testVersionActivity)

	// Test with version 0
	env.ExecuteWorkflow(testWorkflow, "test-change", 0)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result int
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, 0, result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflow)
	env.RegisterActivity(testVersionActivity)

	// Test with version 1
	env.ExecuteWorkflow(testWorkflow, "test-change", 1)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, 1, result)
}

// Helper types for testing
type testHeaderWriter struct {
	headerMap map[string][]byte
}

func (w *testHeaderWriter) Set(key string, value []byte) {
	if value != nil {
		w.headerMap[key] = value
	}
}

type testHeaderReader struct {
	headerMap map[string][]byte
}

func (r *testHeaderReader) Get(key string) ([]byte, bool) {
	value, ok := r.headerMap[key]
	return value, ok
}

func (r *testHeaderReader) ForEachKey(handler func(key string, value []byte) error) error {
	for k, v := range r.headerMap {
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}
