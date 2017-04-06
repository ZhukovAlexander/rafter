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

import sys
from setuptools import setup

# <http://stackoverflow.com/a/13925176/2183102>
if sys.version_info < (3, 5):
    sys.exit("Python 3.5 or later is required")

setup(
    name="rafter",
    keywords="python raft distributed replication",
    packages=['rafter', ],
    install_requires=open('requirements.txt', 'r').read().splitlines(),
    extras_require={'uvloop': ['uvloop']},
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
)
