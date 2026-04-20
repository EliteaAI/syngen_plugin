#!/usr/bin/env python3
"""
Example of enhanced error response structure.
This shows what users will see when an error occurs.
"""

import json

# Example 1: Training failed due to file not found
error_response_1 = {
    "invocation_id": "task_12345",
    "status": "Error",
    "result": json.dumps([
        {
            "object_type": "message",
            "result_target": "response",
            "result_encoding": "plain",
            "data": """Training failed for model 'customer_model'

Error: Failed to save training file: /tmp/workspace/data.csv
Type: RuntimeError
Category: training_failed

Stack Trace:
Traceback (most recent call last):
  File "/app/methods/invoke.py", line 920, in _perform_train
    raise RuntimeError(f"Failed to save training file: {source_path}")
RuntimeError: Failed to save training file: /tmp/workspace/data.csv"""
        }
    ]),
    "result_type": "String",
    "error_category": "training_failed",
    "error_type": "RuntimeError"
}

# Example 2: Inference failed due to memory error
error_response_2 = {
    "invocation_id": "task_67890",
    "status": "Error",
    "result": json.dumps([
        {
            "object_type": "message",
            "result_target": "response",
            "result_encoding": "plain",
            "data": """Data generation failed for model 'large_model'

Error: Training subprocess failed with exit code -9
Type: RuntimeError
Category: out_of_memory

Stack Trace:
Traceback (most recent call last):
  File "/app/methods/invoke.py", line 371, in execute_train
    raise RuntimeError(error_msg)
RuntimeError: Training subprocess failed with exit code -9"""
        }
    ]),
    "result_type": "String",
    "error_category": "out_of_memory",
    "error_type": "RuntimeError"
}

# Example 3: Artifact not found
error_response_3 = {
    "invocation_id": "task_11111",
    "status": "Error",
    "result": json.dumps([
        {
            "object_type": "message",
            "result_target": "response",
            "result_encoding": "plain",
            "data": """Data generation failed for model 'missing_model'

Error: Artifact not found: my-bucket/missing_model_model.tar.gz
Type: RuntimeError
Category: resource_not_found

Stack Trace:
Traceback (most recent call last):
  File "/app/methods/invoke.py", line 567, in download_artifact
    raise RuntimeError(f"Artifact not found: {bucket_name}/{artifact_name}")
RuntimeError: Artifact not found: my-bucket/missing_model_model.tar.gz"""
        }
    ]),
    "result_type": "String",
    "error_category": "resource_not_found",
    "error_type": "RuntimeError"
}

print("=" * 80)
print("Enhanced Error Response Examples")
print("=" * 80)

print("\n📋 Example 1: Training Failed")
print("-" * 80)
print(json.dumps(error_response_1, indent=2))

print("\n\n📋 Example 2: Out of Memory")
print("-" * 80)
print(json.dumps(error_response_2, indent=2))

print("\n\n📋 Example 3: Resource Not Found")
print("-" * 80)
print(json.dumps(error_response_3, indent=2))

print("\n\n✅ Benefits:")
print("-" * 80)
print("1. ✅ Consistent response structure (always has 'status' field)")
print("2. ✅ User-friendly error messages in chat")
print("3. ✅ Full stack trace for debugging")
print("4. ✅ Error categorization (error_category, error_type)")
print("5. ✅ No 'status is missing' errors")
print("6. ✅ Programmatic error handling via error_category field")
