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

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def provider_descriptor(self):
        """ Descriptor """
        service_location_url = self.descriptor.config.get(
            "service_location_url", "http://127.0.0.1:8080"
        )
        #
        return {
            "name": "SyngenServiceProvider",
            "service_location_url": service_location_url,
            "configuration": {},
            "provided_toolkits": [
                {
                    "name": "SyngenToolkit",
                    "description": "Synthetic tabular data generation using VAE models. Generate realistic test datasets from CSV templates while preserving statistical patterns.",
                    "toolkit_config": {
                        "type": "Syngen Configuration",
                        "description": "Configuration for Syngen synthetic data generation.",
                        "fields_order": [
                            "bucket_name",
                        ],
                        "parameters": {
                            "bucket_name": {
                                "type": "String",
                                "required": True,
                                "description": "Artifacts bucket name for storing training data and generated models/outputs",
                            },
                        },
                    },
                    "provided_tools": [
                        {
                            "name": "train_model",
                            "args_schema": {
                                "model_name": {
                                    "type": "String",
                                    "required": True,
                                    "description": "Name for the model (used to identify and retrieve the model later)",
                                },
                                "training_file_name": {
                                    "type": "String",
                                    "required": True,
                                    "description": "Training data CSV file name in the bucket",
                                },
                                "epochs": {
                                    "type": "Integer",
                                    "required": False,
                                    "default": 10,
                                    "description": "Number of training epochs",
                                },
                                "row_limit": {
                                    "type": "Integer",
                                    "required": False,
                                    "description": "Limit number of rows for training (uses all if not set)",
                                },
                                "drop_null": {
                                    "type": "Bool",
                                    "required": False,
                                    "default": False,
                                    "description": "Drop rows with NULL values",
                                },
                                "batch_size": {
                                    "type": "Integer",
                                    "required": False,
                                    "default": 32,
                                    "description": "Training batch size",
                                },
                            },
                            "description": "Train a synthetic data generation model on source CSV data. The trained model can be used later to generate synthetic data.",
                            "tool_metadata": {
                                "result_composition": "list_of_objects",
                                "result_objects": [
                                    {
                                        "object_type": "message",
                                        "result_target": "response",
                                        "result_encoding": "plain"
                                    },
                                    {
                                        "object_type": "model_artifact",
                                        "result_target": "artifact",
                                        "result_extension": "tgz",
                                        "result_encoding": "base64",
                                    }
                                ]
                            },
                            "tool_result_type": "String",
                            "sync_invocation_supported": False,
                            "async_invocation_supported": True,
                        },
                        {
                            "name": "generate_data",
                            # TODO: Add run_parallel parameter back after syngen fixes the parallel inference issue
                            # Currently disabled to avoid user confusion as it doesn't work properly
                            "args_schema": {
                                "model_name": {
                                    "type": "String",
                                    "required": True,
                                    "description": "Name of the trained model to use for generation",
                                },
                                "size": {
                                    "type": "Integer",
                                    "required": False,
                                    "default": 100,
                                    "description": "Number of rows to generate",
                                },
                                "batch_size": {
                                    "type": "Integer",
                                    "required": False,
                                    "default": 32,
                                    "description": "Generation batch size to control memory usage",
                                },
                                "random_seed": {
                                    "type": "Integer",
                                    "required": False,
                                    "description": "Random seed for reproducible results",
                                },
                            },
                            "description": "Generate synthetic data using a previously trained model. Returns a CSV file with the generated data.",
                            "tool_metadata": {
                                "result_composition": "list_of_objects",
                                "result_objects": [
                                    {
                                        "object_type": "message",
                                        "result_target": "response",
                                        "result_encoding": "plain"
                                    },
                                    {
                                        "object_type": "synthetic_data",
                                        "result_target": "artifact",
                                        "result_extension": "csv",
                                        "result_encoding": "base64",
                                    }
                                ]
                            },
                            "tool_result_type": "String",
                            "sync_invocation_supported": False,
                            "async_invocation_supported": True,
                        },
                        {
                            "name": "list_models",
                            "args_schema": {},
                            "description": "List all trained models with their metadata including column names, training parameters, and file information. Returns information about all models stored in the bucket's registry.",
                            "tool_metadata": {
                                "result_composition": "list_of_objects",
                                "result_objects": [
                                    {
                                        "object_type": "message",
                                        "result_target": "response",
                                        "result_encoding": "plain"
                                    }
                                ]
                            },
                            "tool_result_type": "String",
                            "sync_invocation_supported": False,
                            "async_invocation_supported": True,
                        },
                    ],
                    "toolkit_metadata": {},
                },
            ]
        }
