from chainalytic.hub import Launcher


def test_launcher():
    ret = Launcher().launch()
    assert ret == 1
    Launcher().clean()
