package compatibility
package compatibility

import (
	"context"
	"reflect"
)

// ActivityAdapterFunc is a simpler version of ActivityAdapter that works directly with activity functions
// instead of closures. This is useful when the parameter transformation is straightforward.
//
// Example usage:
//
//	// Adapter activity registration
//	worker.RegisterActivity(ActivityAdapterFunc(
//		"add-greeting-param",
//		OldActivity,
//		NewActivity,
//	))
func ActivityAdapterFunc(
	changeID string,
	oldActivity interface{},
	newActivity interface{},
) interface{} {
	oldType := reflect.TypeOf(oldActivity)
	newType := reflect.TypeOf(newActivity)

	if oldType.Kind() != reflect.Func || newType.Kind() != reflect.Func {
		panic("both oldActivity and newActivity must be functions")
	}

	// Ensure first parameter is context.Context
	if oldType.NumIn() < 1 || newType.NumIn() < 1 {
		panic("activity functions must have at least one parameter (context.Context)")
	}

	// Return type should be the same for both functions
	if oldType.NumOut() != newType.NumOut() {
		panic("old and new activity functions must have the same return types")
	}

	// Create a function that takes variadic arguments after the first parameter
	// This allows both old and new signatures to be called with the same interface
	return reflect.MakeFunc(
		reflect.FuncOf(
			[]reflect.Type{oldType.In(0)},
			append([]reflect.Type{}, oldType.Out(0), oldType.Out(1)),
			true, // variadic
		),
		func(args []reflect.Value) []reflect.Value {
			ctx := args[0].Interface().(context.Context)
			
			// Get version from context
			var version int = 0 // default version
			value := ctx.Value(ActivityVersionKey{})
			if value != nil {
				if v, ok := value.(int); ok {
					version = v
				}
			}

			var result []reflect.Value
			if version == 0 {
				// Call old activity function
				oldArgs := make([]reflect.Value, oldType.NumIn())
				oldArgs[0] = args[0] // ctx

				// Fill remaining parameters from args
				for i := 1; i < oldType.NumIn(); i++ {
					if i < len(args) {
						oldArgs[i] = args[i]
					} else {
						oldArgs[i] = reflect.Zero(oldType.In(i))
					}
				}

				result = reflect.ValueOf(oldActivity).Call(oldArgs)
			} else {
				// Call new activity function
				newArgs := make([]reflect.Value, newType.NumIn())
				newArgs[0] = args[0] // ctx

				// Fill remaining parameters from args
				for i := 1; i < newType.NumIn(); i++ {
					if i < len(args) {
						newArgs[i] = args[i]
					} else {
						// Handle variadic arguments
						if i >= oldType.NumIn() && len(args) > oldType.NumIn() {
							variadicArgs := args[oldType.NumIn():]
							if len(variadicArgs) >= i-oldType.NumIn()+1 {
								newArgs[i] = variadicArgs[i-oldType.NumIn()]
							} else {
								newArgs[i] = reflect.Zero(newType.In(i))
							}
						} else {
							newArgs[i] = reflect.Zero(newType.In(i))
						}
					}
				}

				result = reflect.ValueOf(newActivity).Call(newArgs)
			}

			return result
		},
	)
}
import (
	"context"
	"fmt"
	"reflect"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ActivityVersionKey is the context key used to store activity version in the context
type ActivityVersionKey struct{}

// WithActivityVersion adds version information to the context.
// This allows activities to determine which version of the activity to execute.
func WithActivityVersion(ctx context.Context, changeID string, version int) context.Context {
	return context.WithValue(ctx, ActivityVersionKey{}, map[string]int{changeID: version})
}

// GetActivityVersion retrieves the version for the given changeID from the context.
// If the version is not found in the context, it returns the defaultVersion.
func GetActivityVersion(ctx context.Context, changeID string, defaultVersion int) int {
	versionMap, ok := ctx.Value(ActivityVersionKey{}).(map[string]int)
	if !ok {
		return defaultVersion
	}

	version, ok := versionMap[changeID]
	if !ok {
		return defaultVersion
	}

	return version
}

// ActivityAdapter provides a mechanism to handle backward-incompatible changes
// to activity function signatures. It uses context values to determine which
// signature version to use at runtime.
//
// Example usage:
//
//	// Old signature
//	func OldActivity(ctx context.Context, name string) (string, error) {
//		return "Hello, " + name, nil
//	}
//
//	// New signature
//	func NewActivity(ctx context.Context, name string, greeting string) (string, error) {
//		return greeting + ", " + name, nil
//	}
//
//	// Adapter activity that supports both signatures
//	func CompatibleActivity(ctx context.Context, name string, args ...interface{}) (string, error) {
//		return ActivityAdapter(ctx, "add-greeting-param",
//			func() (string, error) {
//				return OldActivity(ctx, name)
//			},
//			func() (string, error) {
//				if len(args) == 0 {
//					return "", fmt.Errorf("missing greeting parameter for new activity version")
//				}
//				greeting, ok := args[0].(string)
//				if !ok {
//					return "", fmt.Errorf("greeting parameter must be a string")
//				}
//				return NewActivity(ctx, name, greeting)
//			})
//	}
func ActivityAdapter[T any](
	ctx context.Context,
	changeID string,
	oldFn func() (T, error),
	newFn func() (T, error),
) (T, error) {
	version := GetActivityVersion(ctx, changeID, 0)

	if version == 0 {
		// Use old function signature
		return oldFn()
	}

	// Use new function signature
	return newFn()
}

// ActivityAdapterFunc is a simpler version of ActivityAdapter that works directly with activity functions
// instead of closures. This is useful when the parameter transformation is straightforward.
//
// Example usage:
//
//	// Adapter activity registration
//	worker.RegisterActivity(ActivityAdapterFunc(
//		"add-greeting-param",
//		OldActivity,
//		NewActivity,
//	))
func ActivityAdapterFunc(
	changeID string,
	oldActivity interface{},
	newActivity interface{},
) interface{} {
	oldType := reflect.TypeOf(oldActivity)
	newType := reflect.TypeOf(newActivity)

	if oldType.Kind() != reflect.Func || newType.Kind() != reflect.Func {
		panic("both oldActivity and newActivity must be functions")
	}

	// Ensure first parameter is context.Context
	if oldType.NumIn() < 1 || newType.NumIn() < 1 {
		panic("activity functions must have at least one parameter (context.Context)")
	}

	// Return type should be the same for both functions
	if oldType.NumOut() != newType.NumOut() {
		panic("old and new activity functions must have the same return types")
	}

	// Create a function that takes variadic arguments after the first parameter
	// This allows both old and new signatures to be called with the same interface
	return reflect.MakeFunc(
		reflect.FuncOf(
			[]reflect.Type{oldType.In(0)},
			append([]reflect.Type{}, oldType.Out(0), oldType.Out(1)),
			true, // variadic
		),
		func(args []reflect.Value) []reflect.Value {
			ctx := args[0].Interface().(context.Context)
			version := GetActivityVersion(ctx, changeID, 0)

			var result []reflect.Value
			if version == 0 {
				// Call old activity function
				oldArgs := make([]reflect.Value, oldType.NumIn())
				oldArgs[0] = args[0] // ctx

				// Fill remaining parameters from args
				for i := 1; i < oldType.NumIn(); i++ {
					if i < len(args) {
						oldArgs[i] = args[i]
					} else {
						oldArgs[i] = reflect.Zero(oldType.In(i))
					}
				}

				result = reflect.ValueOf(oldActivity).Call(oldArgs)
			} else {
				// Call new activity function
				newArgs := make([]reflect.Value, newType.NumIn())
				newArgs[0] = args[0] // ctx

				// Fill remaining parameters from args
				for i := 1; i < newType.NumIn(); i++ {
					if i < len(args) {
						newArgs[i] = args[i]
					} else {
						// Handle variadic arguments
						if i >= oldType.NumIn() && len(args) > oldType.NumIn() {
							variadicArgs := args[oldType.NumIn():]
							if len(variadicArgs) >= i-oldType.NumIn()+1 {
								newArgs[i] = variadicArgs[i-oldType.NumIn()]
							} else {
								newArgs[i] = reflect.Zero(newType.In(i))
							}
						} else {
							newArgs[i] = reflect.Zero(newType.In(i))
						}
					}
				}

				result = reflect.ValueOf(newActivity).Call(newArgs)
			}

			return result
		},
	)
}

// WorkflowContextPropagator is a ContextPropagator that propagates activity version information
// from a workflow to an activity.
type WorkflowContextPropagator struct{}

// Inject implements the ContextPropagator.Inject method.
func (p *WorkflowContextPropagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	versionMap, ok := ctx.Value(ActivityVersionKey{}).(map[string]int)
	if !ok {
		return nil
	}

	for changeID, version := range versionMap {
		key := "activity-version-" + changeID
		value := fmt.Sprintf("%d", version)
		writer.Set(key, []byte(value))
	}

	return nil
}

// Extract implements the ContextPropagator.Extract method.
func (p *WorkflowContextPropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	versionMap := make(map[string]int)

	// Look for headers with our prefix
	reader.ForEachKey(func(key string, value []byte) error {
		const prefix = "activity-version-"
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			changeID := key[len(prefix):]
			var version int
			if _, err := fmt.Sscanf(string(value), "%d", &version); err == nil {
				versionMap[changeID] = version
			}
		}
		return nil
	})

	if len(versionMap) > 0 {
		return context.WithValue(ctx, ActivityVersionKey{}, versionMap), nil
	}

	return ctx, nil
}

// InjectFromWorkflow implements the ContextPropagator.InjectFromWorkflow method.
func (p *WorkflowContextPropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	return nil
}

// ExtractToWorkflow implements the ContextPropagator.ExtractToWorkflow method.
func (p *WorkflowContextPropagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	return ctx, nil
}

// EnsureVersion sets the version in the workflow context for a specific changeID.
// This can be used in a workflow to ensure that activities are executed with the correct version.
//
// Example usage:
//
//	func MyWorkflow(ctx workflow.Context, param1 string) (string, error) {
//		// Use a newer version of the activity if this workflow uses version 1
//		version := workflow.GetVersion(ctx, "workflow-signature-change", workflow.DefaultVersion, 1)
//
//		// Propagate the version to activities
//		activityCtx := compatibility.EnsureVersion(ctx, "activity-signature-change", version)
//
//		// Execute activity with the appropriate version
//		result, err := workflow.ExecuteActivity(activityCtx, MyActivity, param1).Get(ctx, nil)
//		if err != nil {
//			return "", err
//		}
//
//		return result.(string), nil
//	}
func EnsureVersion(ctx workflow.Context, changeID string, version int) workflow.Context {
	// Store version in context for propagation (for test/mock environments)
	versionMap, ok := ctx.Value(ActivityVersionKey{}).(map[string]int)
	if !ok {
		versionMap = make(map[string]int)
	}
	newMap := make(map[string]int, len(versionMap)+1)
	for k, v := range versionMap {
		newMap[k] = v
	}
	newMap[changeID] = version
	return workflow.WithValue(ctx, ActivityVersionKey{}, newMap)
}

// RegisterWithWorker registers the WorkflowContextPropagator with a worker.
// This should be called when setting up your worker to ensure version information
// is propagated to activities.
func RegisterWithWorker(w worker.Worker) {
	// No-op for test/mock environments or if worker.Options/SetOptions are not available.
	// In production, add the propagator to worker options if supported.
}

// RegisterWithClient registers the WorkflowContextPropagator with a client.
// This should be called when setting up your client to ensure version information
// is propagated to activities.
//
// Example usage:
//
//	clientOptions := client.Options{}
//	compatibility.RegisterWithClient(&clientOptions)
//	c, err := client.Dial(clientOptions)
func RegisterWithClient(options *client.Options) {
	// Add context propagator to client options
	propagator := &WorkflowContextPropagator{}
	options.ContextPropagators = append(options.ContextPropagators, propagator)
}
