import sys
from logging import Logger
from typing import Dict, List, Optional, Set, Tuple

from jsonrpcserver import async_dispatch as dispatch

from chainalytic.common import util

EXIT_SERVICE = '__EXIT_SERVICE__'

_LOGGER = None


def set_logger(logger: Logger):
    global _LOGGER
    _LOGGER = logger


async def main_dispatcher(websocket, path):
    response = await dispatch(await websocket.recv())
    if response.wanted:
        await websocket.send(str(response))
        if hasattr(response, 'result'):
            if response.result == EXIT_SERVICE:
                if _LOGGER:
                    _LOGGER.info('Service is terminated')
                sys.exit()


def show_call_info(call_id: str, params: dict):
    if _LOGGER:
        _LOGGER.debug(f'Call: {call_id}')
        _LOGGER.debug('Params:')
        _LOGGER.debug(util.pretty(params))
        _LOGGER.debug('')
