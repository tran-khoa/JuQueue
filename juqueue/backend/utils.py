from __future__ import annotations

from loguru import logger

ALL_EXPERIMENTS = "@ALL_EXPERIMENTS"
ALL_RUNS = "@ALL_RUNS"


def standard_error_handler(_, context):
    if 'exception' in context and context['exception'] is not None:
        logger.opt(exception=context['exception']).error(f"Encountered an unhandled exception "
                                                         f"({type(context['exception'])}: "
                                                         f"{str(context['exception'])}.")
    else:
        logger.error(f"Encountered an exception with error message {context['message']}")
