import os
from ConfigParser import ConfigParser

from ddcsumserver import deserialize
from ddcsumserver.claims_storage import ClaimsStorage
from ddcsumserver.processor import Dispatcher
from .fixtures import raw_tx_with_claim


def _get_config_for_test_storage(tmpdir):
    config = ConfigParser()
    config.add_section('leveldb')
    config.set('leveldb', 'utxo_cache', str(64 * 1024 * 1024))
    config.set('leveldb', 'hist_cache', str(80 * 1024))
    config.set('leveldb', 'addr_cache', str(16 * 1024 * 1024))
    config.set('leveldb', 'claimid_cache', str(16 * 1024 * 1024 * 8))

    config.set('leveldb', 'claim_value_cache', str(1024 * 1024 * 1024))

    config.set('leveldb', 'profiler', 'no')
    config.set('leveldb', 'path', os.path.join(tmpdir.strpath, 'ddcsum_db'))
    return config


def setup_claim_storage(tmpdir):
    config = _get_config_for_test_storage(tmpdir)
    dispatcher = Dispatcher(config)
    shared = dispatcher.shared
    return ClaimsStorage(config, shared, False)


def deserialize_raw_tx(raw_tx):
    vds = deserialize.BCDataStream()
    vds.write(raw_tx.decode('hex'))
    return deserialize.parse_Transaction(vds, is_coinbase=False)


def test_import_claim(tmpdir, benchmark):
    claim_storage = setup_claim_storage(tmpdir)
    txid = "6236b6fc441cf42161051a03041a251f62a23e52b3d3348d6c33244770f265b4"
    block_height = 313486
    tx_with_claim = deserialize_raw_tx(raw_tx_with_claim)
    benchmark(claim_storage.import_claim_transaction, txid, tx_with_claim, block_height)
