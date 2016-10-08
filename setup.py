import sys
from setuptools import setup

# <http://stackoverflow.com/a/13925176/2183102>
if sys.version_info < (3, 5):
    sys.exit("Python 3.5 or later is required")

setup(
    name="rafter",
    version="0.1.0",
    keywords="python raft distributed replication",
    packages=['rafter', ],
    install_requires=open('requirements.txt', 'r').read().splitlines(),
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
)
