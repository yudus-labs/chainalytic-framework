from chainalytic.common import config


class DataFeeder(object):
    """
    Base class for different `DataFeeder` implementations
    """

    def __init__(self):
        super(DataFeeder, self).__init__()
