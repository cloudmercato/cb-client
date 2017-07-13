import os
import json

# Base path of project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Get env vars
ENV_VARS = {
    k.replace('CB_CLIENT_', '').lower(): v
    for k, v in os.environ.items()
    if k.startswith('CB_CLIENT_')
}

DEFAULT_CONFIG_FILES = (
    '/etc/cb_client.json',
    os.path.expanduser('~/.cb_client.json'),
    '.cb_client.json',
)
# Volumes are define by a dict in form:
# {'flavor_id': 1, 'device': '/dev/sdb'}
DEFAULT_CONFIG = {
    'master_url': '',
    'token': '',

    'flavor_id': '',
    'image_id': '',
    'root_volume': {},
    'extra_volumes': [],

    'log_level': 1,
}


def get_config(config_file=None):
    config = DEFAULT_CONFIG.copy()
    if config_file is None:
        for config_file in DEFAULT_CONFIG_FILES:
            if os.path.exists(config_file):
                break
        config.update(json.load(open(config_file)))
    config.update(ENV_VARS)
    return config
