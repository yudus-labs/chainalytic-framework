from pathlib import Path
import shutil
import pytest
from chainalytic.common import config

CHAINALYTIC_TEST = f'{config.CHAINALYTIC_FOLDER}_test'


@pytest.fixture(scope='session')
def setup_chainalytic_config(request):
    print('Setting up temp chainalytic data')

    cur_chainalytic_folder = config.CHAINALYTIC_FOLDER
    config.CHAINALYTIC_FOLDER = CHAINALYTIC_TEST

    cfg = config.init_user_config()
    print('Generated user config for testing')
    yield cfg

    def teardown():
        test_chainalytic_data = Path(config.get_working_dir(), config.CHAINALYTIC_FOLDER,)
        shutil.rmtree(test_chainalytic_data.as_posix(), ignore_errors=1)
        config.CHAINALYTIC_FOLDER = cur_chainalytic_folder
        print('Deleted temp chainalytic data after testing')

    request.addfinalizer(teardown)


@pytest.fixture(scope='session')
def setup_temp_db(request):
    print('Setting up temp leveldb')

    test_chainalytic_folder = CHAINALYTIC_TEST

    db_path = Path(config.CHAINALYTIC_FOLDER).resolve().joinpath('TMP_DB')
    db_path.mkdir(parents=1, exist_ok=1)

    yield db_path.as_posix()

    def teardown():
        test_chainalytic_data = Path(config.get_working_dir(), test_chainalytic_folder)
        shutil.rmtree(test_chainalytic_data.as_posix(), ignore_errors=1)
        print('Deleted temp leveldb after testing')

    request.addfinalizer(teardown)
