import subprocess
import logging

logger = logging.getLogger(__name__)


def create_new_database(db_path):
    '''Runs command to create new database'''
    try:
        r = subprocess.run(
            f'groonga -n {db_path} status'.split(), capture_output=True)
        assert r.returncode == 0
    except AssertionError as e:
        logger.error(r.stderr.decode())
        raise ChildProcessError('Failed to create database')
