from pathlib import Path
import shutil
import pytest
from chainalytic.common import config


@pytest.fixture(scope='session')
def setup_chainalytic_config(request):
    print('Setting up temp chainalytic data')

    cur_chainalytic_folder = config.CHAINALYTIC_FOLDER
    config.CHAINALYTIC_FOLDER = f'{config.CHAINALYTIC_FOLDER}_test'

    cfg = config.init_user_config()
    print('Generated user config for testing')
    yield cfg

    def teardown():
        test_chainalytic_data = Path(
            config.get_working_dir(),
            config.CHAINALYTIC_FOLDER,
        )
        shutil.rmtree(test_chainalytic_data.as_posix(), ignore_errors=1)
        config.CHAINALYTIC_FOLDER = cur_chainalytic_folder
        print('Deleted temp chainalytic data after testing')

    request.addfinalizer(teardown)
