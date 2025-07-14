package compatibility

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
)

// Test workflow that uses the adapter
func testWorkflow(ctx workflow.Context, name string, args ...interface{}) (string, error) {
	return WorkflowAdapter(ctx, "add-greeting-param",
		func() (string, error) {
			return oldWorkflow(ctx, name)
		},
		func() (string, error) {
			if len(args) == 0 {
				return "", errors.New("missing greeting parameter")
			}
			greeting, ok := args[0].(string)
			if !ok {
				return "", errors.New("greeting must be a string")
			}
			return newWorkflow(ctx, name, greeting)
		})
}

// Test workflows
func oldWorkflow(ctx workflow.Context, name string) (string, error) {
	return "Hello, " + name, nil
}

func newWorkflow(ctx workflow.Context, name string, greeting string) (string, error) {
	return greeting + ", " + name, nil
}

func oldWorkflowWithError(ctx workflow.Context, name string) (string, error) {
	if name == "error" {
		return "", errors.New("error requested")
	}
	return "Hello, " + name, nil
}

func newWorkflowWithError(ctx workflow.Context, name string, greeting string) (string, error) {
	if greeting == "error" {
		return "", errors.New("error requested")
	}
	return greeting + ", " + name, nil
}

// Test the WorkflowAdapter function with simple workflow
func TestWorkflowAdapterSimple(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register workflow
	env.RegisterWorkflow(testWorkflow)

	// Test with old version (first execution will be version 0)
	env.ExecuteWorkflow(testWorkflow, "World")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflow)

	// Mock workflow.GetVersion to return version 1
	env.OnWorkflow(testWorkflow, "World", "Greetings").Return("", nil)
	// Simulate workflow.GetVersion returning 1
	env.OnWorkflowGetVersion("add-greeting-param", workflow.DefaultVersion, 1, 1)

	// Test with new version (by forcing GetVersion to return 1)
	env.ExecuteWorkflow(testWorkflow, "World", "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)
}

// Test workflow that uses WorkflowAdapter
func testWorkflowWithAdapter(ctx workflow.Context, name string, args ...interface{}) (string, error) {
	return WorkflowAdapter(ctx, "add-greeting-param",
		func() (string, error) {
			return oldWorkflow(ctx, name)
		},
		func() (string, error) {
			if len(args) == 0 {
				return "", errors.New("missing greeting parameter")
			}
			greeting, ok := args[0].(string)
			if !ok {
				return "", errors.New("greeting must be a string")
			}
			return newWorkflow(ctx, name, greeting)
		})
}

// Test the WorkflowAdapter function
func TestWorkflowAdapter(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Register workflow
	env.RegisterWorkflow(testWorkflowWithAdapter)

	// Test with old version (default)
	env.ExecuteWorkflow(testWorkflowWithAdapter, "World")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflowWithAdapter)

	// Mock GetVersion to return version 1
	env.OnWorkflow(testWorkflowWithAdapter, "World", "Greetings").Return("", nil).
		Run(func(args mock.Arguments) {
			// ctx variable is not needed here since we're not using it
			// Force GetVersion to return 1
			env.OnWorkflowGetVersion("add-greeting-param", workflow.DefaultVersion, 1, 1)
		})

	// Test with new version
	env.ExecuteWorkflow(testWorkflowWithAdapter, "World", "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)

	// Test error handling
	env = testSuite.NewTestWorkflowEnvironment()

	testErrorWorkflow := func(ctx workflow.Context, name string, args ...interface{}) (string, error) {
		return WorkflowAdapter(ctx, "add-greeting-param",
			func() (string, error) {
				return oldWorkflowWithError(ctx, name)
			},
			func() (string, error) {
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return newWorkflowWithError(ctx, name, greeting)
			})
	}

	env.RegisterWorkflow(testErrorWorkflow)

	// Test with old version and error
	env.ExecuteWorkflow(testErrorWorkflow, "error")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "error requested")
}

// Test the WorkflowAdapterFunc function
func TestWorkflowAdapterFunc(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Create an adapter workflow
	adaptedWorkflow := WorkflowAdapterFunc(
		"add-greeting-param",
		oldWorkflow,
		newWorkflow,
	)

	// Register workflow
	env.RegisterWorkflow(adaptedWorkflow)

	// Test with old version (default)
	env.ExecuteWorkflow(adaptedWorkflow, "World")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(adaptedWorkflow)

	// Mock GetVersion to return version 1
	env.OnWorkflow(adaptedWorkflow, "World", "Greetings").Return("", nil).
		Run(func(args mock.Arguments) {
			// Force GetVersion to return 1
			env.OnWorkflowGetVersion(
				"add-greeting-param",
				1,
			)
		})

	// Test with new version
	env.ExecuteWorkflow(adaptedWorkflow, "World", "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)

	// Test error handling
	testErrorWorkflow := func(ctx workflow.Context, name string, args ...interface{}) (string, error) {
		return WorkflowAdapter(ctx, "add-greeting-param",
			func() (string, error) {
				return oldWorkflowWithError(ctx, name)
			},
			func() (string, error) {
				if len(args) == 0 {
					return "", errors.New("missing greeting parameter")
				}
				greeting, ok := args[0].(string)
				if !ok {
					return "", errors.New("greeting must be a string")
				}
				return newWorkflowWithError(ctx, name, greeting)
			})
	}

	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testErrorWorkflow)

	// Test with old version and error
	env.ExecuteWorkflow(testErrorWorkflow, "error")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "error requested")
}

// Test the WorkflowAdapterFunc function again with different setup
func TestWorkflowAdapterFuncAlt(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Create an adapter workflow
	adaptedWorkflow := WorkflowAdapterFunc(
		"add-greeting-param",
		oldWorkflow,
		newWorkflow,
	)

	// Register the workflow
	env.RegisterWorkflow(adaptedWorkflow)

	// Test with old version
	env.ExecuteWorkflow(adaptedWorkflow, "World")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(adaptedWorkflow)

	// Mock workflow.GetVersion to return version 1
	env.OnWorkflow(adaptedWorkflow, "World", "Greetings").Return("", nil).
		Run(func(args mock.Arguments) {
			// Simulate workflow.GetVersion returning 1
			env.OnWorkflowGetVersion(
				"add-greeting-param",
				1,
			)
		})

	// Test with new version (by forcing GetVersion to return 1)
	env.ExecuteWorkflow(adaptedWorkflow, "World", "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)
}

// Additional tests for WorkflowAdapterFunc
func TestAdditionalWorkflowAdapterFunc(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Create an adapter workflow
	adaptedWorkflow := WorkflowAdapterFunc(
		"add-greeting-param",
		oldWorkflow,
		newWorkflow,
	)

	env.RegisterWorkflow(adaptedWorkflow)

	// Test old version
	env.ExecuteWorkflow(adaptedWorkflow, "World")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Hello, World", result)

	// Reset environment
	env = testSuite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(adaptedWorkflow)

	// Test new version (simulate a worker that has been deployed with the new code)
	// Force version to be 1 for this test
	env.OnWorkflow(adaptedWorkflow, mock.Anything, mock.Anything, mock.Anything).Return("", nil).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(workflow.Context)
			// Simulate that workflow.GetVersion returns 1 (new version)
			env.SetLastCompletionResult(map[string]interface{}{"version": 1})
			_ = workflow.GetVersion(ctx, "add-greeting-param", workflow.DefaultVersion, 1)
		})

	env.ExecuteWorkflow(adaptedWorkflow, "World", "Greetings")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Greetings, World", result)
}

// Import github.com/stretchr/testify/mock is used for mocking
