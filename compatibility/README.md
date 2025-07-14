# Compatibility Package for Temporal Go SDK

This package provides a mechanism to handle backward-incompatible changes to workflow and activity function signatures in the Temporal Go SDK, while maintaining compatibility with existing callers.

## Overview

When evolving a Temporal application, you may need to change the signatures of workflow or activity functions. For example, you might need to:

- Add a new parameter
- Remove a parameter
- Change the type of a parameter
- Change the return type

These changes can break backward compatibility, causing existing workflows to fail when they execute with the new code. This package provides adapters that allow you to handle both old and new signatures in the same codebase.

## Workflow Signature Changes

The package provides two main functions for handling workflow signature changes:

1. `WorkflowAdapter`: A generic function that allows you to provide closures for both old and new implementations.
2. `WorkflowAdapterFunc`: A reflection-based adapter that works directly with workflow functions.

### Example: Adding a Parameter
