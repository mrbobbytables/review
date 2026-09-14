#!/usr/bin/env python3
"""The one spelling of the review broker wire protocol (#492).

Re-exports from tui.broker_protocol so both host-side brokers and container TUI
clients share the exact same protocol implementation without divergent behavior.
"""

from __future__ import annotations

import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_image_dir = os.path.join(_repo_root, "image")
if _image_dir not in sys.path:
    sys.path.insert(0, _image_dir)

from tui.broker_protocol import *  # noqa: F401, F403
