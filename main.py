import argparse
import asyncio
from asyncio import new_event_loop, set_event_loop

from config import Config
from engine.manager import MainManager
import logging
from prompt_toolkit import prompt

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="~/.juqueue")
    parser.parse_args()

    main_manager = MainManager(Config.ROOT_DIR / "experiments")
    main_manager.load_experiments()

    loop = new_event_loop()
    set_event_loop(loop)

    while True:
        prompt("Test")
