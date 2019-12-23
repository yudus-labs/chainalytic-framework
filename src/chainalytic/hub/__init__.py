from .chainalytic_hub import ChainalyticHub


class Launcher(object):
    def __init__(self):
        super(Launcher, self).__init__()
        self.hub = ChainalyticHub()

    def launch(self):
        self.hub.init_services()
        print('Launched Chainalytic Hub')
        self.hub.monitor()
        return 1

    def clean(self):
        self.hub.cleanup_services()
        print('Cleaned all Chainalytic Hub services')
        return 1
