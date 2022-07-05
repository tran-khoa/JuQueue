from pathlib import Path

from juqueue.config import Config

DEFAULT_CONFIG = Config(
    def_dir=Path("../example_defs"),
    work_dir=Path("../work"),
    port=8080,
    unix_socket=Path("/tmp/juqueue.sock"),
    debug=False
)
