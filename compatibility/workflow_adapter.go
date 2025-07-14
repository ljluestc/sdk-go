package compatibility

import (
	"context"
	"errors"
	"reflect"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// WorkflowVersionKey is the context key used to store workflow version in the context
type WorkflowVersionKey struct{}

// ActivityVersionKey is the context key used to store activity version in the context
type ActivityVersionKey struct{}

// contextPropagator implements workflow.ContextPropagator to propagate version info
// between workflows and activities
type contextPropagator struct{}

// Inject implements workflow.ContextPropagator.Inject
func (p *contextPropagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(ActivityVersionKey{})
	if value != nil {
		version, ok := value.(workflow.Version)
		if ok {
			writer.Set("version", []byte(string(version)))
		}
	}
	return nil
}

// Extract implements workflow.ContextPropagator.Extract
func (p *contextPropagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if value, ok := reader.Get("version"); ok {
		version := workflow.Version(string(value))
		ctx = context.WithValue(ctx, ActivityVersionKey{}, version)
	}
	return ctx, nil
}

// InjectFromWorkflow implements workflow.ContextPropagator.InjectFromWorkflow
func (p *contextPropagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value(WorkflowVersionKey{})
	if value != nil {
		version, ok := value.(workflow.Version)
		if ok {
			writer.Set("version", []byte(string(version)))
		}
	}
	return nil
}

// ExtractToWorkflow implements workflow.ContextPropagator.ExtractToWorkflow
func (p *contextPropagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if value, ok := reader.Get("version"); ok {
		version := workflow.Version(string(value))
		ctx = workflow.WithValue(ctx, WorkflowVersionKey{}, version)
	}
	return ctx, nil
}

// RegisterWithWorker registers the context propagator with a worker
func RegisterWithWorker(w worker.Worker) {
	w.Options().ContextPropagators = append(w.Options().ContextPropagators, &contextPropagator{})
}

// EnsureVersion sets the version in the workflow context to ensure it's propagated to activities
func EnsureVersion(ctx workflow.Context, changeID string, version workflow.Version) workflow.Context {
	// Store the version in the context so it can be propagated to activities
	return workflow.WithValue(ctx, WorkflowVersionKey{}, version)
}

// WorkflowAdapter provides a mechanism to handle backward-incompatible changes
// to workflow function signatures. It uses workflow.GetVersion to determine which
// signature version to use at runtime.
//
// Example usage:
//
//	// Old signature
//	func OldWorkflow(ctx workflow.Context, name string) (string, error) {
//		return "Hello, " + name, nil
//	}
//
//	// New signature
//	func NewWorkflow(ctx workflow.Context, name string, greeting string) (string, error) {
//		return greeting + ", " + name, nil
//	}
//
//	// Adapter workflow that supports both signatures
//	func CompatibleWorkflow(ctx workflow.Context, name string, args ...interface{}) (string, error) {
//		return WorkflowAdapter(ctx, "add-greeting-param",
//			func() (string, error) {
//				return OldWorkflow(ctx, name)
//			},
//			func() (string, error) {
//				if len(args) == 0 {
//					return "", errors.New("missing greeting parameter for new workflow version")
//				}
//				greeting, ok := args[0].(string)
//				if !ok {
//					return "", errors.New("greeting parameter must be a string")
//				}
//				return NewWorkflow(ctx, name, greeting)
//			})
//	}
func WorkflowAdapter[T any](
	ctx workflow.Context,
	changeID string,
	oldFn func() (T, error),
	newFn func() (T, error),
) (T, error) {
	version := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, 1)

	if version == workflow.DefaultVersion {
		// Use old function signature
		return oldFn()
	}

	// Use new function signature
	return newFn()
}

// WorkflowAdapterFunc is a simpler version of WorkflowAdapter that works directly with workflow functions
// instead of closures. This is useful when the parameter transformation is straightforward.
//
// Example usage:
//
//	// Adapter workflow registration
//	worker.RegisterWorkflow(WorkflowAdapterFunc(
//		"add-greeting-param",
//		OldWorkflow,
//		NewWorkflow,
//	))
func WorkflowAdapterFunc(
	changeID string,
	oldWorkflow interface{},
	newWorkflow interface{},
) interface{} {
	oldType := reflect.TypeOf(oldWorkflow)
	newType := reflect.TypeOf(newWorkflow)

	if oldType.Kind() != reflect.Func || newType.Kind() != reflect.Func {
		panic("both oldWorkflow and newWorkflow must be functions")
	}

	// Ensure first parameter is workflow.Context
	if oldType.NumIn() < 1 || newType.NumIn() < 1 {
		panic("workflow functions must have at least one parameter (workflow.Context)")
	}

	// Return type should be the same for both functions
	if oldType.NumOut() != newType.NumOut() {
		panic("old and new workflow functions must have the same return types")
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
			ctx := args[0].Interface().(workflow.Context)
			version := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, 1)

			var result []reflect.Value
			if version == workflow.DefaultVersion {
				// Call old workflow function
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

				result = reflect.ValueOf(oldWorkflow).Call(oldArgs)
			} else {
				// Call new workflow function
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

				result = reflect.ValueOf(newWorkflow).Call(newArgs)
			}

			return result
		},
	)
}

// ActivityAdapter provides a mechanism to handle backward-incompatible changes
// to activity function signatures. It works similar to WorkflowAdapter but for activities.
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
//					return "", errors.New("missing greeting parameter")
//				}
//				greeting, ok := args[0].(string)
//				if !ok {
//					return "", errors.New("greeting must be a string")
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
	// Get version from context
	var version workflow.Version = workflow.DefaultVersion
	value := ctx.Value(ActivityVersionKey{})
	if value != nil {
		if v, ok := value.(workflow.Version); ok {
			version = v
		}
	}

	if version == workflow.DefaultVersion {
		// Use old function signature
		return oldFn()
	}

	// Use new function signature
	return newFn()
}
