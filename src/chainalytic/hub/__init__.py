from .chainalytic_hub import ChainalyticHub


class Launcher(object):
    def __init__(self):
        super(Launcher, self).__init__()
        self.hub = ChainalyticHub()

    def launch(self):
        self.hub.init_services()
        print('Launched Chainalytic Hub')
        return 1
