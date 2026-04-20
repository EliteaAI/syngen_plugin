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
import pathlib

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
    def runtime_config(self):
        """ Method """
        config_maps = [
            lambda result: {
                "base_path": str(pathlib.Path(__file__).parent.parent.joinpath("data", "syngen")),
            },
            lambda result: {
                "workspace_home": os.path.join(result["base_path"], "workspace"),
                "models_home": os.path.join(result["base_path"], "models"),
            },
        ]
        #
        # Default docker settings
        default_docker = {
            "enabled": True,
            "container_name": "syngen_runner",
            "syngen_artifacts_path": "/src/model_artifacts",
            "docker_socket": "/var/run/docker.sock",
            # Command format: "docker" for tdspora/syngen image (python3 -m start --task=)
            #                 "cli" for pip-installed syngen (train/infer commands)
            "command_format": "docker",
        }
        #
        result = {}
        #
        for config_map in config_maps:
            for key, default in config_map(result).items():
                result[key] = self.descriptor.config.get(key, default)
        #
        # Merge docker config (from descriptor.config or defaults)
        docker_config = self.descriptor.config.get("docker", {})
        result["docker"] = {**default_docker, **docker_config}
        #
        return result
