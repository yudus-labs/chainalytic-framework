import pytest
from chainalytic import config


def test_chain_registry_value():
    zones = config.CHAIN_REGISTRY['zones']
    for z in zones:
        if z['zone_id'] == 'public-icon':
            assert z['zone_name'] == 'Public ICON mainnet'
            assert z['client_endpoint'] == 'https://ctz.solidwallet.io'
            assert z['storage_location'] == ''
        elif z['zone_id'] == 'public-ethereum':
            assert z['zone_name'] == 'Public Ethereum mainnet'
            assert z['client_endpoint'] == ''
            assert z['storage_location'] == ''


def test_setting_value():
    valid_keys = [
        'aggregator_endpoint',
        'warehouse_endpoint',
        'provider_endpoint',
        'warehouse_root',
        'storage_root',
    ]
    for k in valid_keys:
        assert k in config.SETTING

    assert config.SETTING['aggregator_endpoint'] == 'localhost:5500'
    assert config.SETTING['warehouse_endpoint'] == 'localhost:5510'
    assert config.SETTING['provider_endpoint'] == 'localhost:5520'
    assert config.SETTING['warehouse_root'] == '.chainalytic_warehouse'
    assert config.SETTING['storage_root'] == '{zone_id}_storage'
