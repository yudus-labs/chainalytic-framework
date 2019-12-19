from chainalytic.common import config
from chainalytic.aggregator import Aggregator
from chainalytic.warehouse import Warehouse
from chainalytic.provider import Provider


class ChainalyticHub(object):
    """
    Main hub
    """

    def __init__(self):
        super(ChainalyticHub, self).__init__()
