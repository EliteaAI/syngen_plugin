#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Method """

import os
import json
import base64
import shutil
import tarfile
import pathlib
import uuid
import re
import sys
from datetime import datetime

import requests

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611

# Regex to strip ANSI escape codes
ANSI_ESCAPE_PATTERN = re.compile(r'\x1b\[[0-9;]*m')


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def extract_artifact_settings(self, llm_settings):
        """
        Extract artifact API settings from llm_settings.
        
        llm_settings contains:
        - openai_api_base: e.g., 'http://<host_name>/llm/v1'
        - openai_api_key: API key (same key works for artifacts)
        - openai_organization: project_id (e.g., '2')
        
        Returns dict with base_url, api_key, project_id for artifact API calls.
        """
        openai_api_base = llm_settings.get("openai_api_base", "")
        openai_api_key = llm_settings.get("openai_api_key", "")
        openai_organization = llm_settings.get("openai_organization", "")
        #
        # Strip /llm/v1 or similar suffix from openai_api_base to get base URL
        # e.g., 'http://<host_name>/llm/v1' -> 'http://<host_name>'
        base_url = re.sub(r'/llm/v\d+/?$', '', openai_api_base)
        #
        return {
            "base_url": base_url,
            "api_key": openai_api_key,
            "project_id": openai_organization,
            "api_path": "/api/v1",
            "x_secret": llm_settings.get("x_secret", "secret"),
        }

    @web.method()
    def download_artifact(self, artifact_settings, bucket_name, artifact_name):
        """ Download artifact from platform bucket """
        base_url = artifact_settings.get("base_url", "")
        api_path = artifact_settings.get("api_path", "/api/v1")
        project_id = artifact_settings.get("project_id", "")
        api_key = artifact_settings.get("api_key", "")
        #
        artifact_url = f"{base_url}{api_path}/artifacts/artifact/default/{project_id}"
        url = f"{artifact_url}/{bucket_name.lower()}/{artifact_name}"
        #
        headers = {
            "Authorization": f"Bearer {api_key}",
            "X-SECRET": artifact_settings.get("x_secret", "secret"),
        }
        #
        log.info("Downloading artifact: %s", url)
        #
        response = requests.get(url, headers=headers, verify=False, timeout=300)
        #
        if response.status_code == 403:
            raise RuntimeError("Not authorized to access artifact")
        elif response.status_code == 404:
            raise RuntimeError(f"Artifact not found: {bucket_name}/{artifact_name}")
        elif response.status_code != 200:
            raise RuntimeError(f"Failed to download artifact: {response.status_code}")
        #
        # Platform returns raw bytes directly (no base64 encoding)
        content = response.content
        log.info("Downloaded artifact: %d bytes", len(content))
        #
        return content

    @web.method()
    def upload_artifact(self, artifact_settings, bucket_name, artifact_name, artifact_data):
        """ Upload artifact to platform bucket """
        base_url = artifact_settings.get("base_url", "")
        api_path = artifact_settings.get("api_path", "/api/v1")
        project_id = artifact_settings.get("project_id", "")
        api_key = artifact_settings.get("api_key", "")
        #
        artifacts_url = f"{base_url}{api_path}/artifacts/artifacts/default/{project_id}"
        url = f"{artifacts_url}/{bucket_name.lower()}"
        #
        headers = {
            "Authorization": f"Bearer {api_key}",
            "X-SECRET": artifact_settings.get("x_secret", "secret"),
        }
        #
        log.info("Uploading artifact: %s to bucket %s", artifact_name, bucket_name)
        #
        files = {'file': (artifact_name, artifact_data)}
        response = requests.post(url, headers=headers, files=files, verify=False, timeout=300)
        #
        if response.status_code == 403:
            raise RuntimeError("Not authorized to upload artifact")
        elif response.status_code not in [200, 201]:
            raise RuntimeError(f"Failed to upload artifact: {response.status_code}")
        #
        log.info("Uploaded artifact successfully")
        return response.json()

    @web.method()
    def _load_registry(self, artifact_settings, bucket_name):
        """
        Load the models registry JSON from bucket.
        Returns empty registry structure if file doesn't exist.
        """
        registry_name = "_syngen_models_registry.json"
        try:
            data = self.download_artifact(artifact_settings, bucket_name, registry_name)
            registry = json.loads(data.decode('utf-8'))
            log.info("Loaded registry with %d models", len(registry.get("models", {})))
            return registry
        except RuntimeError as e:
            error_msg = str(e).lower()
            # Handle both 404 (not found) and 400 (file doesn't exist yet) errors
            if "not found" in error_msg or "400" in error_msg:
                log.info("Registry not found, creating new one")
                return {"models": {}, "version": "1.0"}
            raise

    @web.method()
    def _save_registry(self, artifact_settings, bucket_name, registry):
        """
        Save the models registry JSON to bucket.
        """
        registry_name = "_syngen_models_registry.json"
        registry_json = json.dumps(registry, indent=2)
        self.upload_artifact(artifact_settings, bucket_name, registry_name, registry_json.encode('utf-8'))
        log.info("Saved registry with %d models", len(registry.get("models", {})))

    @web.method()
    def _update_registry_entry(self, artifact_settings, bucket_name, model_name, model_metadata, max_retries=3):
        """
        Update or add a model entry in the registry.
        Uses retry logic to handle concurrent updates.
        """
        import time
        #
        for attempt in range(max_retries):
            try:
                # Load current registry
                registry = self._load_registry(artifact_settings, bucket_name)
                #
                # Update model entry
                if "models" not in registry:
                    registry["models"] = {}
                #
                registry["models"][model_name] = model_metadata
                #
                # Save registry
                self._save_registry(artifact_settings, bucket_name, registry)
                #
                log.info("Updated registry entry for model: %s", model_name)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    log.error("Failed to update registry after %d attempts: %s", max_retries, e)
                    raise RuntimeError(f"Failed to update model registry: {e}")
                #
                log.warning("Registry update attempt %d failed, retrying... Error: %s", attempt + 1, e)
                time.sleep(0.5)

    @web.method()
    def _extract_columns_from_file(self, file_path):
        """
        Extract column names from training file using pandas.
        Supports CSV, Excel (.xlsx, .xls), and Avro formats.
        """
        import pandas as pd
        #
        file_ext = os.path.splitext(file_path)[1].lower()
        #
        try:
            if file_ext in ['.csv', '.txt', '.tsv', '.psv']:
                # For CSV files
                separator = '\t' if file_ext == '.tsv' else '|' if file_ext == '.psv' else ','
                df_head = pd.read_csv(file_path, sep=separator, nrows=0)
                columns = df_head.columns.tolist()
            elif file_ext in ['.xlsx', '.xls']:
                # For Excel files
                df_head = pd.read_excel(file_path, nrows=0)
                columns = df_head.columns.tolist()
            elif file_ext == '.avro':
                # For Avro files, need pandavro
                try:
                    import pandavro as pdx
                    import fastavro
                    #
                    with open(file_path, "rb") as f:
                        reader = fastavro.reader(f)
                        schema = reader.writer_schema
                        # Extract field names from Avro schema
                        columns = [field["name"] for field in schema.get("fields", [])]
                except ImportError:
                    log.warning("pandavro not available, cannot extract Avro columns")
                    columns = []
            else:
                log.warning("Unsupported file format: %s", file_ext)
                columns = []
            #
            log.info("Extracted %d columns from %s", len(columns), file_path)
            return columns
        except Exception as e:
            log.error("Failed to extract columns from %s: %s", file_path, e)
            return []

    @web.method()
    def _create_error_response(self, invocation_id, operation, model_name, exception, include_traceback=True):
        """
        Create a structured error response with optional stack trace.
        
        Args:
            invocation_id: The invocation ID
            operation: Operation name (e.g., 'training', 'inference', 'list_models')
            model_name: Model name if applicable
            exception: The exception object
            include_traceback: Whether to include stack trace in response (default: True)
        
        Returns:
            dict: Structured error response
        """
        import traceback
        
        # Categorize error type
        error_type = type(exception).__name__
        error_category = "unknown_error"
        
        if "not found" in str(exception).lower() or isinstance(exception, FileNotFoundError):
            error_category = "resource_not_found"
        elif "download" in str(exception).lower() or "artifact" in str(exception).lower():
            error_category = "artifact_error"
        elif "memory" in str(exception).lower() or isinstance(exception, MemoryError):
            error_category = "out_of_memory"
        elif "timeout" in str(exception).lower():
            error_category = "timeout_error"
        elif isinstance(exception, RuntimeError):
            if "training" in str(exception).lower():
                error_category = "training_failed"
            elif "inference" in str(exception).lower() or "generat" in str(exception).lower():
                error_category = "inference_failed"
            else:
                error_category = "runtime_error"
        elif isinstance(exception, ValueError):
            error_category = "invalid_input"
        
        # Build user-friendly message
        model_context = f" for model '{model_name}'" if model_name else ""
        error_message = f"{operation.capitalize()} failed{model_context}\n\n"
        error_message += f"Error: {str(exception)}\n"
        error_message += f"Type: {error_type}\n"
        error_message += f"Category: {error_category}"
        
        # Add stack trace if requested
        if include_traceback:
            tb_lines = traceback.format_exception(type(exception), exception, exception.__traceback__)
            stack_trace = "".join(tb_lines)
            error_message += f"\n\nStack Trace:\n{stack_trace}"
        
        # Create result objects
        result_objects = [
            {
                "object_type": "message",
                "result_target": "response",
                "result_encoding": "plain",
                "data": error_message
            }
        ]
        
        return {
            "invocation_id": invocation_id,
            "status": "Error",
            "result": json.dumps(result_objects),
            "result_type": "String",
            "error_category": error_category,  # Extra field for programmatic error handling
            "error_type": error_type,  # Extra field for error type
        }

    @web.method()
    def validate_invoke_request(self, toolkit_name, tool_name, request_data):
        """ Invoke: validate """
        #
        # Check toolkit/tool
        #
        if toolkit_name != "SyngenToolkit" or tool_name not in [
                "train_model", "generate_data", "list_models",
        ]:
            return {
                "errorCode": 404,
                "message": "Resource not found",
                "details": [f"Unknown toolkit/tool: {toolkit_name}/{tool_name}"],
            }
        #
        # Check params
        #
        toolkit_params = request_data.get("configuration", {}).get("parameters", {})
        tool_params = request_data.get("parameters", {})
        #
        params = toolkit_params.copy()
        for key, value in tool_params.items():
            if key not in params or value:
                params[key] = value
        #
        # Required toolkit params (configuration-level)
        required_params = ["llm_settings", "bucket_name"]
        #
        # Additional required toolkit params for train_model
        if tool_name == "train_model":
            # training_file_name is a toolkit config param
            required_params.append("training_file_name")
        #
        for key in required_params:
            if key not in params or not params[key]:
                return {
                    "errorCode": 400,
                    "message": "Missing required parameter",
                    "details": [f"Parameter '{key}' is required"],
                }
        #
        return None

    @web.method()
    def perform_invoke_request(self, toolkit_name, tool_name, request_data):  # pylint: disable=R0912,R0914,R0915
        """ Invoke: perform """
        work_path = None
        #
        try:
            #
            # Input data
            #
            toolkit_params = request_data.get("configuration", {}).get("parameters", {})
            tool_params = request_data.get("parameters", {})
            #
            params = toolkit_params.copy()
            for key, value in tool_params.items():
                if key not in params or value:
                    params[key] = value
            #
            llm_settings = params.get("llm_settings", {})
            bucket_name = params["bucket_name"]
            #
            # model_name is not required for list_models
            model_name = params.get("model_name") if tool_name != "list_models" else None
            #
            # Extract artifact settings from llm_settings
            artifact_settings = self.extract_artifact_settings(llm_settings)
            #
            # list_models doesn't need workspace setup
            if tool_name == "list_models":
                return self._perform_list_models(
                    params, artifact_settings, bucket_name
                )
            #
            # Workspace and config (for train_model and generate_data)
            #
            config = self.runtime_config()
            #
            workspace_home = config["workspace_home"]
            pathlib.Path(workspace_home).mkdir(parents=True, exist_ok=True)
            #
            # Create unique work directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            work_path = os.path.join(workspace_home, f"{model_name}_{timestamp}_{unique_id}")
            pathlib.Path(work_path).mkdir(parents=True, exist_ok=True)
            #
            # Set up model artifacts path within work directory
            model_artifacts_path = os.path.join(work_path, "model_artifacts")
            pathlib.Path(model_artifacts_path).mkdir(parents=True, exist_ok=True)
            #
            self.invocation_stop_checkpoint()
            #
            if tool_name == "train_model":
                return self._perform_train(
                    params, artifact_settings, bucket_name, model_name,
                    work_path, model_artifacts_path
                )
            elif tool_name == "generate_data":
                return self._perform_generate(
                    params, artifact_settings, bucket_name, model_name,
                    work_path, model_artifacts_path
                )
            #
        except BaseException as exception:  # pylint: disable=W0718
            # Catch unexpected errors not handled by _perform_* methods
            log.exception("Unexpected error in invoke %s:%s", toolkit_name, tool_name)
            
            # Get invocation_id if available
            try:
                import tasknode_task  # pylint: disable=E0401,C0415
                invocation_id = tasknode_task.id
            except:
                invocation_id = "unknown"
            
            # Return structured error response with full details
            return self._create_error_response(
                invocation_id=invocation_id,
                operation=f"{toolkit_name}.{tool_name}",
                model_name=params.get("model_name") if 'params' in locals() else None,
                exception=exception,
                include_traceback=True  # Always include trace for unexpected errors
            )
        finally:
            if work_path is not None:
                try:
                    shutil.rmtree(work_path)
                except:  # pylint: disable=W0702
                    pass

    @web.method()
    def _perform_train(self, params, artifact_settings, bucket_name, model_name, work_path, model_artifacts_path):  # pylint: disable=R0914
        """ Perform training using syngen Worker library """
        import tasknode_task  # pylint: disable=E0401,C0415
        invocation_id = tasknode_task.id
        
        try:
            # Get training parameters
            training_file_name = params.get("training_file_name")
            epochs = params.get("epochs", 10)
            row_limit = params.get("row_limit")
            drop_null = params.get("drop_null", False)
            batch_size = params.get("batch_size", 32)
            #
            # Download training file from bucket using artifact API
            self.invocation_thinking(f"Downloading training file: {training_file_name}")
            self.invocation_stop_checkpoint()
            #
            training_data = self.download_artifact(artifact_settings, bucket_name, training_file_name)
            #
            log.info("Downloaded artifact size: %d bytes", len(training_data))
            #
            # Save training file locally in work directory
            source_path = os.path.join(work_path, training_file_name)
            with open(source_path, "wb") as f:
                f.write(training_data)
            #
            # Verify file was saved
            if os.path.exists(source_path):
                file_size = os.path.getsize(source_path)
                log.info("Training file saved to: %s (size: %d bytes)", source_path, file_size)
            else:
                log.error("Failed to save training file to: %s", source_path)
                raise RuntimeError(f"Failed to save training file: {source_path}")
            #
            # Apply row_limit preprocessing (syngen's row_limit doesn't work reliably)
            self.invocation_stop_checkpoint()
            import pandas as pd
            #
            file_ext = os.path.splitext(source_path)[1].lower()
            total_rows = 0
            actual_training_rows = 0
            #
            # Read dataset to check row count and apply limit
            if file_ext in ['.csv', '.txt', '.tsv', '.psv']:
                separator = '\t' if file_ext == '.tsv' else '|' if file_ext == '.psv' else ','
                df = pd.read_csv(source_path, sep=separator)
                total_rows = len(df)
                #
                # Apply row limit if specified and less than total rows
                if row_limit is not None and row_limit > 0 and row_limit < total_rows:
                    df = df.head(row_limit)
                    actual_training_rows = len(df)
                    # Save truncated dataset back
                    df.to_csv(source_path, sep=separator, index=False)
                    log.info("Applied row_limit: truncated dataset from %d to %d rows", total_rows, actual_training_rows)
                    self.invocation_thinking(f"Training will use {actual_training_rows} rows (limited from {total_rows} total rows in dataset)")
                else:
                    actual_training_rows = total_rows
                    if row_limit is not None and row_limit >= total_rows:
                        self.invocation_thinking(f"Training will use all {total_rows} rows (requested limit {row_limit} >= dataset size)")
                    else:
                        self.invocation_thinking(f"Training will use all {total_rows} rows from the dataset")
            elif file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(source_path)
                total_rows = len(df)
                #
                if row_limit is not None and row_limit > 0 and row_limit < total_rows:
                    df = df.head(row_limit)
                    actual_training_rows = len(df)
                    df.to_excel(source_path, index=False)
                    log.info("Applied row_limit: truncated dataset from %d to %d rows", total_rows, actual_training_rows)
                    self.invocation_thinking(f"Training will use {actual_training_rows} rows (limited from {total_rows} total rows in dataset)")
                else:
                    actual_training_rows = total_rows
                    if row_limit is not None and row_limit >= total_rows:
                        self.invocation_thinking(f"Training will use all {total_rows} rows (requested limit {row_limit} >= dataset size)")
                    else:
                        self.invocation_thinking(f"Training will use all {total_rows} rows from the dataset")
            elif file_ext == '.avro':
                # Handle Avro files using fastavro
                try:
                    import fastavro
                    #
                    # Read all records to count and potentially truncate
                    with open(source_path, 'rb') as f:
                        reader = fastavro.reader(f)
                        schema = reader.writer_schema
                        records = list(reader)
                    #
                    total_rows = len(records)
                    #
                    if row_limit is not None and row_limit > 0 and row_limit < total_rows:
                        # Truncate records
                        records = records[:row_limit]
                        actual_training_rows = len(records)
                        # Write truncated records back
                        with open(source_path, 'wb') as f:
                            fastavro.writer(f, schema, records)
                        log.info("Applied row_limit: truncated Avro dataset from %d to %d rows", total_rows, actual_training_rows)
                        self.invocation_thinking(f"Training will use {actual_training_rows} rows (limited from {total_rows} total rows in dataset)")
                    else:
                        actual_training_rows = total_rows
                        if row_limit is not None and row_limit >= total_rows:
                            self.invocation_thinking(f"Training will use all {total_rows} rows (requested limit {row_limit} >= dataset size)")
                        else:
                            self.invocation_thinking(f"Training will use all {total_rows} rows from the dataset")
                except ImportError:
                    log.warning("fastavro not available, cannot apply row_limit to Avro file")
                    self.invocation_thinking(f"Starting training for model: {model_name}")
                except Exception as e:
                    log.warning("Failed to apply row_limit to Avro file: %s", e)
                    self.invocation_thinking(f"Starting training for model: {model_name}")
            else:
                # For other formats, skip row limit preprocessing
                log.warning("Row limit preprocessing not supported for format: %s", file_ext)
                self.invocation_thinking(f"Starting training for model: {model_name}")
            #
            self.invocation_thinking(f"Starting training for model: {model_name} (epochs={epochs}, batch_size={batch_size})")
            self.invocation_stop_checkpoint()
            #
            # Run training via subprocess (non-blocking, async-friendly)
            # This calls process.py which handles the subprocess and thinking callbacks
            model_path, actual_model_dir = self.run_syngen_train(
                work_path=work_path,
                table_name=model_name,
                source_file=source_path,
                epochs=epochs,
                batch_size=batch_size,
                drop_null=drop_null
            )
            #
            log.info("Model trained at: %s, actual dir name: %s", model_path, actual_model_dir)
            self.invocation_stop_checkpoint()
            #
            # Verify model was created - search for it in various possible locations
            if not os.path.exists(model_path):
                # List what we have in work_path for debugging
                log.error("Model path not found: %s", model_path)
                log.error("Work path contents: %s", os.listdir(work_path) if os.path.exists(work_path) else "N/A")
                #
                # Check model_artifacts structure
                model_artifacts_dir = os.path.join(work_path, "model_artifacts")
                if os.path.exists(model_artifacts_dir):
                    log.error("model_artifacts contents: %s", os.listdir(model_artifacts_dir))
                    # Check resources subfolder
                    resources_dir = os.path.join(model_artifacts_dir, "resources")
                    if os.path.exists(resources_dir):
                        log.error("model_artifacts/resources contents: %s", os.listdir(resources_dir))
                #
                # Check top-level resources
                if os.path.exists(os.path.join(work_path, "resources")):
                    log.error("resources contents: %s", os.listdir(os.path.join(work_path, "resources")))
                #
                # Try to find the model directory anywhere in work_path
                # Note: syngen converts underscores to hyphens in directory names
                model_name_hyphen = model_name.replace('_', '-')
                search_names = {model_name, model_name_hyphen}
                log.info("Searching for model directory '%s' or '%s' in work_path...", model_name, model_name_hyphen)
                for root, dirs, files in os.walk(work_path):
                    for dir_name in dirs:
                        if dir_name in search_names:
                            found_path = os.path.join(root, dir_name)
                            log.info("Found model directory at: %s", found_path)
                            model_path = found_path
                            break
                    if os.path.exists(model_path):
                        break
                #
                # If still not found, raise error
                if not os.path.exists(model_path):
                    raise RuntimeError(f"Model not found at: {model_path}")
            #
            # Package model artifacts
            self.invocation_thinking("Packaging model artifacts...")
            #
            # Create tar.gz archive of model - include both resources and tmp_store
            # Note: Using .tgz extension to avoid platform issues with double extensions (.tar.gz)
            model_archive_name = f"{model_name}_model.tgz"
            model_archive_path = os.path.join(work_path, model_archive_name)
            #
            with tarfile.open(model_archive_path, "w:gz") as tar:
                # Add the model from resources/{actual_model_dir}
                # IMPORTANT: Keep the actual syngen directory name (with hyphens) to preserve train_message.success path
                tar.add(model_path, arcname=actual_model_dir)
                log.info("Added model directory as: %s", actual_model_dir)
                
                # Also add tmp_store if it exists (contains input_data pkl for reports)
                # Note: syngen might use hyphenated name here too
                tmp_store_candidates = [
                    os.path.join(work_path, "model_artifacts", "tmp_store", actual_model_dir),
                    os.path.join(work_path, "model_artifacts", "tmp_store", model_name),
                ]
                tmp_store_path = None
                for candidate in tmp_store_candidates:
                    if os.path.exists(candidate):
                        tmp_store_path = candidate
                        break
                
                if tmp_store_path:
                    # Add with path structure: tmp_store/{actual_model_dir}/...
                    tar.add(tmp_store_path, arcname=os.path.join("tmp_store", actual_model_dir))
                    log.info("Included tmp_store in model archive from: %s", tmp_store_path)
                else:
                    log.warning("tmp_store not found in candidates: %s", tmp_store_candidates)
            #
            # Read and encode model archive
            with open(model_archive_path, "rb") as f:
                model_data = f.read()
            #
            model_base64 = base64.b64encode(model_data).decode("ascii")
            #
            # Extract columns from training file
            self.invocation_thinking("Extracting column metadata from training file...")
            columns = self._extract_columns_from_file(source_path)
            #
            # Update model registry
            self.invocation_thinking("Updating model registry...")
            model_metadata = {
                "model_name": model_name,
                "columns": columns,
                "model_file_name": model_archive_name,
                "training_file_name": training_file_name,
                "training_params": {
                    "epochs": epochs,
                    "batch_size": batch_size,
                    "row_limit": row_limit,
                    "drop_null": drop_null
                },
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            #
            try:
                self._update_registry_entry(artifact_settings, bucket_name, model_name, model_metadata)
                log.info("Registry updated successfully for model: %s", model_name)
            except Exception as e:
                log.warning("Failed to update registry (non-fatal): %s", e)
            #
            # Build result
            output_message = f"Training completed for model '{model_name}'. Model saved as {model_archive_name}."
            #
            result_objects = [
                {
                    "object_type": "message",
                    "result_target": "response",
                    "result_encoding": "plain",
                    "data": output_message
                },
                {
                    "name": model_archive_name,
                    "object_type": "model_artifact",
                    "result_target": "artifact",
                    "result_extension": "tgz",
                    "result_encoding": "base64",
                    "result_bucket": bucket_name,
                    "data": model_base64
                }
            ]
            #
            return {
                "invocation_id": invocation_id,
                "status": "Completed",
                "result": json.dumps(result_objects),
                "result_type": "String",
            }
        except Exception as e:
            log.exception("Training failed: %s", e)
            return self._create_error_response(
                invocation_id=invocation_id,
                operation="training",
                model_name=model_name,
                exception=e,
                include_traceback=True  # Include stack trace for debugging
            )

    @web.method()
    def _perform_generate(self, params, artifact_settings, bucket_name, model_name, work_path, model_artifacts_path):  # pylint: disable=R0914
        """ Perform data generation using syngen Worker library """
        import tasknode_task  # pylint: disable=E0401,C0415
        invocation_id = tasknode_task.id
        
        try:
            # Get generation parameters
            size = params.get("size", 100)
            batch_size = params.get("batch_size", 32)
            random_seed = params.get("random_seed")
            #
            # TODO: Add run_parallel parameter back after syngen fixes the parallel inference issue
            # For now, hardcoded to False to avoid multiprocessing issues with random_seed_list attribute
            run_parallel = False
            #
            # Download model artifact from bucket using artifact API
            # Note: Using .tgz extension to match training output
            model_archive_name = f"{model_name}_model.tgz"
            #
            self.invocation_thinking(f"Downloading model: {model_archive_name}")
            self.invocation_stop_checkpoint()
            #
            model_data = self.download_artifact(artifact_settings, bucket_name, model_archive_name)
            #
            # Save and extract model archive in work directory
            model_archive_path = os.path.join(work_path, model_archive_name)
            with open(model_archive_path, "wb") as f:
                f.write(model_data)
            #
            # Extract model to work_path/model_artifacts/
            # Archive contains: {model_name}/ (resources) and tmp_store/{model_name}/ (input data)
            model_artifacts_path = os.path.join(work_path, "model_artifacts")
            os.makedirs(model_artifacts_path, exist_ok=True)
            
            # First extract to temp location to see structure
            import tempfile
            actual_model_dir_name = None  # Track the actual name used by syngen
            
            with tempfile.TemporaryDirectory() as temp_dir:
                # Try to open as tar.gz first, fall back to plain tar
                try:
                    with tarfile.open(model_archive_path, "r:gz") as tar:
                        tar.extractall(temp_dir)
                    log.info("Extracted model archive as gzip tar")
                except tarfile.ReadError:
                    log.info("Not a gzip file, trying plain tar...")
                    with tarfile.open(model_archive_path, "r:") as tar:
                        tar.extractall(temp_dir)
                    log.info("Extracted model archive as plain tar")
                
                # Log what was extracted
                log.info("Temp dir contents after extraction: %s", os.listdir(temp_dir))
                
                # Move extracted files to correct locations
                import shutil
                
                # Move model directory to resources/
                # Note: syngen converts underscores to hyphens, so check both variants
                # IMPORTANT: Keep the original name from archive - syngen expects it
                resources_path = os.path.join(model_artifacts_path, "resources")
                os.makedirs(resources_path, exist_ok=True)
                
                # Look for model directory with both underscore and hyphen names
                model_name_hyphen = model_name.replace('_', '-')
                temp_model_path = None
                
                for name in [model_name_hyphen, model_name]:  # Check hyphen first (syngen's preferred)
                    candidate = os.path.join(temp_dir, name)
                    if os.path.exists(candidate):
                        temp_model_path = candidate
                        actual_model_dir_name = name
                        log.info("Found extracted model directory: %s", name)
                        break
                
                if temp_model_path and os.path.exists(temp_model_path):
                    # Keep the original name - don't rename!
                    target_model_path = os.path.join(resources_path, actual_model_dir_name)
                    if os.path.exists(target_model_path):
                        shutil.rmtree(target_model_path)
                    shutil.move(temp_model_path, target_model_path)
                    log.info("Moved model to: %s (keeping original name '%s')", target_model_path, actual_model_dir_name)
                    
                    # Verify train_message.success exists - this is what syngen checks for
                    success_file = os.path.join(target_model_path, "train_message.success")
                    if os.path.exists(success_file):
                        with open(success_file, 'r') as f:
                            content = f.read().strip()
                        log.info("Found train_message.success with content: '%s'", content)
                    else:
                        log.warning("train_message.success NOT found at: %s", success_file)
                        # List what IS in the model directory
                        log.warning("Model directory contents: %s", os.listdir(target_model_path))
                else:
                    log.error("Model directory not found in archive. Temp contents: %s", os.listdir(temp_dir))
                    actual_model_dir_name = model_name  # Fallback
                
                # Move tmp_store/ to model_artifacts/tmp_store/
                # Keep original names - don't rename!
                temp_tmp_store = os.path.join(temp_dir, "tmp_store")
                if os.path.exists(temp_tmp_store):
                    log.info("tmp_store contents: %s", os.listdir(temp_tmp_store))
                    target_tmp_store = os.path.join(model_artifacts_path, "tmp_store")
                    if os.path.exists(target_tmp_store):
                        shutil.rmtree(target_tmp_store)
                    shutil.move(temp_tmp_store, target_tmp_store)
                    log.info("Moved tmp_store to: %s", target_tmp_store)
            
            resources_path = os.path.join(model_artifacts_path, "resources")
            #
            log.info("Model extracted to: %s", resources_path)
            log.info("Resources path contents: %s", os.listdir(resources_path))
            log.info("Using table_name for inference: %s", actual_model_dir_name)
            #
            # Verify model exists using actual extracted name
            model_dir = os.path.join(resources_path, actual_model_dir_name)
            if not os.path.exists(model_dir):
                log.error("Model directory not found: %s", model_dir)
                log.error("Resources path contents: %s", os.listdir(resources_path) if os.path.exists(resources_path) else "N/A")
                raise RuntimeError(f"Model directory not found: {model_dir}")
            #
            self.invocation_thinking(f"Generating {size} rows for model: {model_name}")
            self.invocation_stop_checkpoint()
            #
            # Run inference via subprocess (non-blocking, async-friendly)
            # IMPORTANT: Use the actual model directory name (with hyphens) for syngen
            output_dir = self.run_syngen_infer(
                work_path=work_path,
                table_name=actual_model_dir_name,  # Use the actual name syngen created
                size=size,
                batch_size=batch_size,
                run_parallel=run_parallel,
                random_seed=random_seed
            )
            #
            self.invocation_stop_checkpoint()
            #
            # Find generated CSV file
            # Worker outputs to work_path/tmp_store/{table_name}/merged_infer_{table_name}.csv
            # Note: Use actual_model_dir_name since that's what syngen uses
            generated_csv_path = os.path.join(output_dir, f"merged_infer_{actual_model_dir_name}.csv")
            #
            if not os.path.exists(generated_csv_path):
                # Try to find any CSV in the output directory
                if os.path.exists(output_dir):
                    log.info("Output directory contents: %s", os.listdir(output_dir))
                    for f in os.listdir(output_dir):
                        if f.endswith(".csv"):
                            generated_csv_path = os.path.join(output_dir, f)
                            log.info("Found CSV: %s", generated_csv_path)
                            break
            #
            if not os.path.exists(generated_csv_path):
                log.error("Generated CSV not found at: %s", generated_csv_path)
                log.error("Output directory: %s", output_dir)
                if os.path.exists(output_dir):
                    log.error("Output directory contents: %s", os.listdir(output_dir))
                raise RuntimeError(f"Generated CSV not found at expected path: {generated_csv_path}")
            #
            self.invocation_thinking("Reading generated data...")
            #
            # Read and encode generated CSV
            with open(generated_csv_path, "rb") as f:
                csv_data = f.read()
            #
            csv_base64 = base64.b64encode(csv_data).decode("ascii")
            #
            # Build result
            csv_filename = f"{model_name}_synthetic.csv"
            output_message = f"Generated {size} rows of synthetic data for model '{model_name}'. Output: {csv_filename}"
            #
            result_objects = [
                {
                    "object_type": "message",
                    "result_target": "response",
                    "result_encoding": "plain",
                    "data": output_message
                },
                {
                    "name": csv_filename,
                    "object_type": "synthetic_data",
                    "result_target": "artifact",
                    "result_extension": "csv",
                    "result_encoding": "base64",
                    "result_bucket": bucket_name,
                    "data": csv_base64
                }
            ]
            #
            return {
                "invocation_id": invocation_id,
                "status": "Completed",
                "result": json.dumps(result_objects),
                "result_type": "String",
            }
        except Exception as e:
            log.exception("Data generation failed: %s", e)
            return self._create_error_response(
                invocation_id=invocation_id,
                operation="data generation",
                model_name=model_name,
                exception=e,
                include_traceback=True  # Include stack trace for debugging
            )

    @web.method()
    def _perform_list_models(self, params, artifact_settings, bucket_name):
        """ Perform list_models - retrieve and display all trained models """
        import tasknode_task  # pylint: disable=E0401,C0415
        invocation_id = tasknode_task.id
        
        try:
            self.invocation_thinking("Loading models registry...")
            #
            # Load registry
            registry = self._load_registry(artifact_settings, bucket_name)
            models = registry.get("models", {})
            #
            if not models:
                output_message = "No trained models found in the registry."
            else:
                # Format models list
                output_lines = [f"Found {len(models)} trained model(s):\n"]
                #
                for idx, (model_name, metadata) in enumerate(sorted(models.items()), 1):
                    output_lines.append(f"\n{idx}. Model: {model_name}")
                    output_lines.append(f"   - Columns ({len(metadata.get('columns', []))}): {', '.join(metadata.get('columns', []))}")
                    output_lines.append(f"   - Training file: {metadata.get('training_file_name', 'N/A')}")
                    output_lines.append(f"   - Model file: {metadata.get('model_file_name', 'N/A')}")
                    #
                    training_params = metadata.get('training_params', {})
                    if training_params:
                        output_lines.append(f"   - Training params:")
                        output_lines.append(f"     • Epochs: {training_params.get('epochs', 'N/A')}")
                        output_lines.append(f"     • Batch size: {training_params.get('batch_size', 'N/A')}")
                        if training_params.get('row_limit'):
                            output_lines.append(f"     • Row limit: {training_params['row_limit']}")
                        output_lines.append(f"     • Drop null: {training_params.get('drop_null', False)}")
                    #
                    if metadata.get('created_at'):
                        output_lines.append(f"   - Created: {metadata['created_at']}")
                    if metadata.get('updated_at') and metadata.get('updated_at') != metadata.get('created_at'):
                        output_lines.append(f"   - Updated: {metadata['updated_at']}")
                #
                output_message = "\n".join(output_lines)
            #
            log.info("Listed %d models from registry", len(models))
            #
            # Build result
            result_objects = [
                {
                    "object_type": "message",
                    "result_target": "response",
                    "result_encoding": "plain",
                    "data": output_message
                }
            ]
            #
            return {
                "invocation_id": invocation_id,
                "status": "Completed",
                "result": json.dumps(result_objects),
                "result_type": "String",
            }
        except Exception as e:
            log.exception("List models failed: %s", e)
            return self._create_error_response(
                invocation_id=invocation_id,
                operation="list models",
                model_name=None,
                exception=e,
                include_traceback=True
            )
