// Package compatibility provides tools for handling backward-incompatible changes
// to workflow and activity function signatures in the Temporal Go SDK.
//
// The main components of this package are:
//
// 1. WorkflowAdapter and WorkflowAdapterFunc for handling workflow signature changes
// 2. ActivityAdapter and ActivityAdapterFunc for handling activity signature changes
// 3. Context propagation utilities for passing version information between workflows and activities
//
// This package allows you to evolve your code over time by changing function signatures
// while maintaining compatibility with existing workflows and activities.
//
// Example usage for workflows:
//
//	func CompatibleWorkflow(ctx workflow.Context, name string, args ...interface{}) (string, error) {
//		return compatibility.WorkflowAdapter(ctx, "add-greeting-param",
//			func() (string, error) {
//				return OldWorkflow(ctx, name)
//			},
//			func() (string, error) {
//				greeting := args[0].(string)
//				return NewWorkflow(ctx, name, greeting)
//			})
//	}
//
// Example usage for activities:
//
//	func CompatibleActivity(ctx context.Context, name string, args ...interface{}) (string, error) {
//		return compatibility.ActivityAdapter(ctx, "add-greeting-param",
//			func() (string, error) {
//				return OldActivity(ctx, name)
//			},
//			func() (string, error) {
//				greeting := args[0].(string)
//				return NewActivity(ctx, name, greeting)
//			})
//	}
package compatibility
