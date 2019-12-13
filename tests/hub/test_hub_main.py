from chainalytic.hub import launcher


def test_launcher():
    ret = launcher.launch()
    assert ret == 1
