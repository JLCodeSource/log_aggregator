import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__),
                                 '..')))
from aggregator import config  # noqa

settings = config.get_settings()
