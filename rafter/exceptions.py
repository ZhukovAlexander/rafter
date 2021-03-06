# Copyright 2017 Alexander Zhukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class NotLeaderException(Exception):
    """Raised when server is not a leader"""


class UnboundExposedCommand(Exception):
    """Raised when the command is not bound to a service instance"""


class UnknownCommand(Exception):
    """Raised when someont tries to invoka an unknown command"""
