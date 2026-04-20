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

""" Route """

import flask  # pylint: disable=E0401

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Route:  # pylint: disable=E1101,R0903
    """ Route """

    @web.route("/tools/<toolkit_name>/<tool_name>/invocations/<invocation_id>", methods=["GET", "DELETE"])  # pylint: disable=C0301
    def invocations_route(self, toolkit_name, tool_name, invocation_id):  # pylint: disable=R0911
        """ Handler """
        if flask.request.method == "GET":
            with self.state_lock:
                if toolkit_name not in self.invocation_state:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                if tool_name not in self.invocation_state[toolkit_name]:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                if invocation_id not in self.invocation_state[toolkit_name][tool_name]:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                invocation_state = self.invocation_state[toolkit_name][tool_name][invocation_id]
                invocation_status = invocation_state["status"]
                #
                custom_events = {}
                #
                if "custom_events" in invocation_state and invocation_state["custom_events"]:
                    custom_events["custom_events"] = invocation_state["custom_events"].copy()
                    invocation_state["custom_events"].clear()
                #
                if invocation_status == "pending":
                    return {
                        "invocation_id": invocation_id,
                        "status": "Started",
                        **custom_events,
                    }
                #
                if invocation_status == "running":
                    return {
                        "invocation_id": invocation_id,
                        "status": "InProgress",
                        **custom_events,
                    }
                #
                if "result" in invocation_state:
                    return invocation_state["result"]
        #
        elif flask.request.method == "DELETE":
            with self.state_lock:
                if toolkit_name not in self.invocation_state:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                if tool_name not in self.invocation_state[toolkit_name]:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                if invocation_id not in self.invocation_state[toolkit_name][tool_name]:
                    return {
                        "errorCode": "404",
                        "message": "Resource Not Found",
                        "details": [],
                    }, 404
                #
                invocation_state = self.invocation_state[toolkit_name][tool_name][invocation_id]
                invocation_state["stop_requested"] = True
                #
                if "processes" in invocation_state:
                    for proc in invocation_state["processes"]:
                        if proc.poll() is None:
                            proc.terminate()
            #
            return flask.Response(status=204)
        #
        return {
            "errorCode": "500",
            "message": "Internal Server Error",
            "details": [],
        }, 500
