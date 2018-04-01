import hashlib
import os
from Queue import Queue
import random
import sys
import time
import threading
import traceback

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

from ddcsumserver import deserialize
from ddcsumserver.processor import Processor, print_log
from ddcsumserver.claims_storage import ClaimsStorage
from ddcsumserver.utils import logger, hash_decode, hash_encode, Hash, header_from_string
from ddcsumserver.utils import header_to_string, ProfiledThread, rev_hex, int_to_hex, PoWHash

from ddcsschema.uri import parse_ddcs_uri
from ddcsschema.error import URIParseError, DecodeError
from ddcsschema.decode import smart_decode

HEADER_SIZE = 112
BLOCKS_PER_CHUNK = 96

# This determines the max uris that can be requested
# in a single batch command
MAX_BATCH_URIS = 500

CLAIM_ID = "claim_id"
WINNING = "winning"
SEQUENCE = "sequence"


def command(cmd_name):
    def _wrapper(fn):
        setattr(fn, '_is_command', True)
        setattr(fn, '_command_name', cmd_name)
        return fn
    return _wrapper


def setup_caching(config):
    cache_type = config.get('caching', 'type')
    data_dir = config.get('caching', 'data_dir')
    short_expire = config.get('caching', 'short_expire')
    long_expire = config.get('caching', 'long_expire')

    cache_opts = {
        'cache.type': cache_type,
        'cache.data_dir': data_dir,
        'cache.lock_dir': data_dir,
        'cache.regions': 'short_term, long_term',
        'cache.short_term.type': cache_type,
        'cache.short_term.expire': short_expire,
        'cache.long_term.type': cache_type,
        'cache.long_term.expire': long_expire,
    }

    cache_manager = CacheManager(**parse_cache_config_options(cache_opts))
    short_term_cache = cache_manager.get_cache('short_term', expire=short_expire)
    long_term_cache = cache_manager.get_cache('long_term', expire=long_expire)
    return short_term_cache, long_term_cache


def ddcscrd_proof_has_winning_claim(proof):
    return 'txhash' in proof and 'nOut' in proof


class BlockchainProcessorBase(Processor):
    def __init__(self, config, shared):
        Processor.__init__(self)

        # monitoring
        self.short_term_cache, self.long_term_cache = setup_caching(config)
        self.avg_time = 0, 0, 0
        self.time_ref = time.time()

        self.shared = shared
        self.config = config
        self.up_to_date = False

        self.watch_lock = threading.Lock()
        self.watch_blocks = []
        self.watch_headers = []
        self.watched_addresses = {}

        self.history_cache = {}
        self.merkle_cache = {}
        self.max_cache_size = 100000
        self.chunk_cache = {}
        self.cache_lock = threading.Lock()
        self.headers_data = ''
        self.headers_path = config.get('leveldb', 'path')

        self.mempool_values = {}
        self.mempool_addresses = {}
        self.mempool_hist = {}  # addr -> (txid, delta)
        self.mempool_hashes = set()
        self.mempool_lock = threading.Lock()

        self.address_queue = Queue()

        try:
            self.test_reorgs = config.getboolean('leveldb', 'test_reorgs')  # simulate random blockchain reorgs
        except:
            self.test_reorgs = False
        self.storage = ClaimsStorage(config, shared, self.test_reorgs)

        self.ddcscrdd_url = 'http://%s:%s@%s:%s/' % (
            config.get('ddcscrdd', 'ddcscrdd_user'),
            config.get('ddcscrdd', 'ddcscrdd_password'),
            config.get('ddcscrdd', 'ddcscrdd_host'),
            config.get('ddcscrdd', 'ddcscrdd_port'))

        self.sent_height = 0
        self.sent_header = None

        # catch_up headers
        self.init_headers(self.storage.height)
        # start catch_up thread
        if config.getboolean('leveldb', 'profiler'):
            filename = os.path.join(config.get('leveldb', 'path'), 'profile')
            print_log('profiled thread', filename)
            self.blockchain_thread = ProfiledThread(filename, target=self.do_catch_up)
        else:
            self.blockchain_thread = threading.Thread(target=self.do_catch_up)
        self.blockchain_thread.start()

    def do_catch_up(self):
        self.header = self.block2header(self.ddcscrdd('getblock', (self.storage.last_hash,)))
        self.header['utxo_root'] = self.storage.get_root_hash().encode('hex')
        self.catch_up(sync=False)
        if not self.shared.stopped():
            print_log("Blockchain is up to date.")
            self.memorypool_update()
            print_log("Memory pool initialized.")

        while not self.shared.stopped():
            self.main_iteration()
            if self.shared.paused():
                print_log("ddcscrdd is responding")
                self.shared.unpause()
            time.sleep(0.1)

    def set_time(self):
        self.time_ref = time.time()

    def print_time(self, num_tx):
        delta = time.time() - self.time_ref
        # leaky averages
        seconds_per_block, tx_per_second, n = self.avg_time
        alpha = (1. + 0.01 * n) / (n + 1)
        seconds_per_block = (1 - alpha) * seconds_per_block + alpha * delta
        alpha2 = alpha * delta / seconds_per_block
        tx_per_second = (1 - alpha2) * tx_per_second + alpha2 * num_tx / delta
        self.avg_time = seconds_per_block, tx_per_second, n + 1
        if self.storage.height % 100 == 0 \
                or (self.storage.height % 10 == 0 and self.storage.height >= 100000) \
                or self.storage.height >= 200000:
            msg = "block %d (%d %.2fs) %s" % (
                self.storage.height, num_tx, delta, self.storage.get_root_hash().encode('hex'))
            msg += " (%.2ftx/s, %.2fs/block)" % (tx_per_second, seconds_per_block)
            run_blocks = self.storage.height - self.start_catchup_height
            remaining_blocks = self.ddcscrdd_height - self.storage.height
            if run_blocks > 0 and remaining_blocks > 0:
                remaining_minutes = remaining_blocks * seconds_per_block / 60
                new_blocks = int(remaining_minutes / 10)  # number of new blocks expected during catchup
                blocks_to_process = remaining_blocks + new_blocks
                minutes = blocks_to_process * seconds_per_block / 60
                rt = "%.0fmin" % minutes if minutes < 300 else "%.1f hours" % (minutes / 60)
                msg += " (eta %s, %d blocks)" % (rt, remaining_blocks)
            print_log(msg)

    def wait_on_ddcscrdd(self):
        self.shared.pause()
        time.sleep(0.1)
        if self.shared.stopped():
            # this will end the thread
            raise BaseException()

    def get_claims_for_name(self, claim_name):
        cache, cache_key = self.short_term_cache, 'getclaimsforname' + claim_name
        if cache_key in cache: return cache.get(cache_key)
        ddcscrdd_results = self.ddcscrdd("getclaimsforname", (claim_name, ))
        cache.put(cache_key, ddcscrdd_results)
        return ddcscrdd_results

    def get_raw_transaction(self, tx_hash):
        cache, cache_key = self.long_term_cache, 'getrawtransaction' + tx_hash
        if cache_key in cache: return cache.get(cache_key)
        ddcscrdd_results = self.ddcscrdd('getrawtransaction', (tx_hash, 0))
        cache.put(cache_key, ddcscrdd_results)
        return ddcscrdd_results

    def ddcscrdd(self, method, args=()):
        while True:
            try:
                r = AuthServiceProxy(self.ddcscrdd_url, method).__call__(*args)
                return r
            except JSONRPCException as j:
                r = "no response"
                print_log("Failed: %s%s" % (method, args))
                if j.error['code'] == -28:
                    print_log("ddcscrdd still warming up...")
                    self.wait_on_ddcscrdd()
                    continue
                elif j.error['code'] == -343:
                    print_log("missing JSON-RPC result")
                    raise BaseException(j.error)
                elif j.error['code'] == -342:
                    print_log("missing HTTP response from server")
                    raise BaseException(j.error)
                elif j.error['code'] == -1:
                    print_log("JSON value is not a string as expected: %s" % j)
                    raise BaseException(j.error)
                else:
                    print_log(
                        "While calling %s(%s): JSONRPCException: " % (method, args),
                        j.error['message'])
                    raise BaseException(j.error)

    @staticmethod
    def block2header(b):
        return {
            "block_height": b.get('height'),
            "version": b.get('version'),
            "prev_block_hash": b.get('previousblockhash'),
            "merkle_root": b.get('merkleroot'),
            "claim_trie_root": b.get('nameclaimroot'),
            "timestamp": b.get('time'),
            "bits": int(b.get('bits'), 16),
            "nonce": b.get('nonce'),
        }

    def get_header(self, height):
        block_hash = self.ddcscrdd('getblockhash', (height,))
        b = self.ddcscrdd('getblock', (block_hash,))
        return self.block2header(b)

    def init_headers(self, db_height):
        self.headers_filename = os.path.join(self.headers_path, 'blockchain_headers')

        if os.path.exists(self.headers_filename):
            height = os.path.getsize(self.headers_filename) / HEADER_SIZE - 1  # the current height
            if height > 0:
                prev_hash = self.hash_header(self.read_header(height))
            else:
                prev_hash = None
        else:
            open(self.headers_filename, 'wb').close()
            prev_hash = None
            height = -1

        if height < db_height:
            print_log("catching up missing headers:", height, db_height)

        try:
            while height < db_height:
                height += 1
                header = self.get_header(height)
                if height > 1:
                    if prev_hash != header.get('prev_block_hash'):
                        # The prev_hash block is orphaned, go back
                        print_log("reorganizing, a block in file is orphaned:", prev_hash)
                        # Go to the parent of the orphaned block
                        height -= 2
                        prev_hash = self.hash_header(self.read_header(height))
                        continue

                self.write_header(header, sync=False)
                prev_hash = self.hash_header(header)
                if (height % 1000) == 0:
                    print_log("headers file:", height)
        except KeyboardInterrupt:
            self.flush_headers()
            sys.exit()

        self.flush_headers()

    @staticmethod
    def hash_header(header):
        return rev_hex(Hash(header_to_string(header).decode('hex')).encode('hex'))

    def read_header(self, block_height):
        if os.path.exists(self.headers_filename):
            with open(self.headers_filename, 'rb') as f:
                f.seek(block_height * HEADER_SIZE)
                h = f.read(HEADER_SIZE)
            if len(h) == HEADER_SIZE:
                h = header_from_string(h)
                return h

    def read_chunk(self, index):
        with open(self.headers_filename, 'rb') as f:
            f.seek(index * BLOCKS_PER_CHUNK * HEADER_SIZE)
            chunk = f.read(BLOCKS_PER_CHUNK * HEADER_SIZE)
        return chunk.encode('hex')

    def write_header(self, header, sync=True):
        if not self.headers_data:
            self.headers_offset = header.get('block_height')

        self.headers_data += header_to_string(header).decode('hex')
        if sync or len(self.headers_data) > 40 * 100:
            self.flush_headers()

        with self.cache_lock:
            chunk_index = header.get('block_height') / BLOCKS_PER_CHUNK
            if self.chunk_cache.get(chunk_index):
                self.chunk_cache.pop(chunk_index)

    def pop_header(self):
        # we need to do this only if we have not flushed
        if self.headers_data:
            self.headers_data = self.headers_data[:-40]

    def flush_headers(self):
        if not self.headers_data:
            return
        with open(self.headers_filename, 'rb+') as f:
            f.seek(self.headers_offset * HEADER_SIZE)
            f.write(self.headers_data)
        self.headers_data = ''

    def get_chunk(self, i):
        # store them on disk; store the current chunk in memory
        with self.cache_lock:
            chunk = self.chunk_cache.get(i)
            if not chunk:
                chunk = self.read_chunk(i)
                if chunk:
                    self.chunk_cache[i] = chunk

        return chunk

    def get_mempool_transaction(self, txid):
        try:
            raw_tx = self.ddcscrdd('getrawtransaction', (txid, 0))
        except:
            print_log("Error looking up txid: %s" % txid)
            return None

        vds = deserialize.BCDataStream()
        vds.write(raw_tx.decode('hex'))
        try:
            return deserialize.parse_Transaction(vds, is_coinbase=False)
        except:
            print_log("ERROR: cannot parse", txid)
            return None

    def get_history(self, addr, cache_only=False):
        with self.cache_lock:
            hist = self.history_cache.get(addr)
        if hist is not None:
            return sorted(hist, key=lambda x: x['height'])
        if cache_only:
            return -1

        hist = self.storage.get_history(addr)

        # add memory pool
        with self.mempool_lock:
            for txid, delta in self.mempool_hist.get(addr, ()):
                hist.append({'tx_hash': txid, 'height': 0})

        with self.cache_lock:
            if len(self.history_cache) > self.max_cache_size:
                logger.info("clearing cache")
                self.history_cache.clear()
            self.history_cache[addr] = hist
        return sorted(hist, key=lambda x: x['height'])

    def get_unconfirmed_history(self, addr):
        hist = []
        with self.mempool_lock:
            for txid, delta in self.mempool_hist.get(addr, ()):
                hist.append({'tx_hash': txid, 'height': 0})
        return sorted(hist, key=lambda x: x['height'])

    def get_unconfirmed_value(self, addr):
        v = 0
        with self.mempool_lock:
            for txid, delta in self.mempool_hist.get(addr, ()):
                v += delta
        return v

    def get_status(self, addr, cache_only=False):
        tx_points = self.get_history(addr, cache_only)
        if cache_only and tx_points == -1:
            return -1

        if not tx_points:
            return None
        if tx_points == ['*']:
            return '*'
        status = ''.join(tx.get('tx_hash') + ':%d:' % tx.get('height') for tx in tx_points)
        return hashlib.sha256(status).digest().encode('hex')

    def get_merkle(self, tx_hash, height, cache_only):
        with self.cache_lock:
            out = self.merkle_cache.get(tx_hash)
        if out is not None:
            return out
        if cache_only:
            return -1

        block_hash = self.ddcscrdd('getblockhash', (height,))
        b = self.ddcscrdd('getblock', (block_hash,))
        tx_list = b.get('tx')
        tx_pos = tx_list.index(tx_hash)

        merkle = map(hash_decode, tx_list)
        target_hash = hash_decode(tx_hash)
        s = []
        while len(merkle) != 1:
            if len(merkle) % 2:
                merkle.append(merkle[-1])
            n = []
            while merkle:
                new_hash = Hash(merkle[0] + merkle[1])
                if merkle[0] == target_hash:
                    s.append(hash_encode(merkle[1]))
                    target_hash = new_hash
                elif merkle[1] == target_hash:
                    s.append(hash_encode(merkle[0]))
                    target_hash = new_hash
                n.append(new_hash)
                merkle = merkle[2:]
            merkle = n

        out = {"block_height": height, "merkle": s, "pos": tx_pos}
        with self.cache_lock:
            if len(self.merkle_cache) > self.max_cache_size:
                logger.info("clearing merkle cache")
                self.merkle_cache.clear()
            self.merkle_cache[tx_hash] = out
        return out

    @staticmethod
    def deserialize_block(block):
        txlist = block.get('tx')
        tx_hashes = []  # ordered txids
        txdict = {}  # deserialized tx
        is_coinbase = True
        for raw_tx in txlist:
            tx_hash = hash_encode(Hash(raw_tx.decode('hex')))
            vds = deserialize.BCDataStream()
            vds.write(raw_tx.decode('hex'))
            try:
                tx = deserialize.parse_Transaction(vds, is_coinbase)
            except:
                print_log("ERROR: cannot parse", tx_hash)
                continue
            tx_hashes.append(tx_hash)
            txdict[tx_hash] = tx
            is_coinbase = False
        return tx_hashes, txdict


    def import_block(self, block, block_hash, block_height, revert=False):
        self.short_term_cache.clear()

        touched_addr = set()

        # deserialize transactions
        tx_hashes, txdict = self.deserialize_block(block)

        # undo info
        if revert:
            undo_info = self.storage.get_undo_info(block_height)
            claim_undo_info = self.storage.get_undo_claim_info(block_height)
            tx_hashes.reverse()
        else:
            undo_info = {}
            claim_undo_info = {}
        for txid in tx_hashes:  # must be ordered
            tx = txdict[txid]
            if not revert:
                undo = self.storage.import_transaction(txid, tx, block_height, touched_addr)
                undo_info[txid] = undo

                undo = self.storage.import_claim_transaction(txid, tx, block_height)
                claim_undo_info[txid] = undo
            else:
                undo = undo_info.pop(txid)
                self.storage.revert_transaction(txid, tx, block_height, touched_addr, undo)
                undo = claim_undo_info.pop(txid)
                self.storage.revert_claim_transaction(undo)

        if revert:
            assert claim_undo_info == {}
            assert undo_info == {}

        # add undo info
        if not revert:
            self.storage.write_undo_info(block_height, undo_info)
            self.storage.write_undo_claim_info(block_height, claim_undo_info)

        # add the max
        self.storage.save_height(block_hash, block_height)

        for addr in touched_addr:
            self.invalidate_cache(addr)

        self.storage.update_hashes()
        # batch write modified nodes 
        self.storage.batch_write()
        # return length for monitoring
        return len(tx_hashes)

    def add_request(self, session, request):
        # see if we can get if from cache. if not, add request to queue
        message_id = request.get('id')
        try:
            result = self.process(request, cache_only=False)
        except BaseException as e:
            print_log("Bad request from", session.address, str(type(e)), ":", str(e))
            traceback.print_exc()
            self.push_response(session, {'id': message_id, 'error': str(e)})
            return
        except:
            logger.error("process error", exc_info=True)
            print_log("error processing request from", session.address)
            print_log(str(request))
            self.push_response(session, {'id': message_id, 'error': 'unknown error'})

        if result == -1:
            self.queue.put((session, request))
        else:
            self.push_response(session, {'id': message_id, 'result': result})

    def get_claim_info(self, claim_id):
        result = {}
        claim_id = str(claim_id)
        logger.debug("get_claim_info claim_id:{}".format(claim_id))
        claim_name = self.storage.get_claim_name(claim_id)
        claim_value = self.storage.get_claim_value(claim_id)
        claim_out = self.storage.get_outpoint_from_claim_id(claim_id)
        claim_height = self.storage.get_claim_height(claim_id)
        claim_address = self.storage.get_claim_address(claim_id)
        if claim_name and claim_id:
            claim_sequence = self.storage.get_n_for_name_and_claimid(claim_name, claim_id)
        else:
            claim_sequence = None
        if None not in (claim_name, claim_value, claim_out, claim_height, claim_sequence):
            claim_txid, claim_nout, claim_amount = claim_out
            claim_value = claim_value.encode('hex')
            result = {
                "name": claim_name,
                "claim_id": claim_id,
                "txid": claim_txid,
                "nout": claim_nout,
                "amount":claim_amount,
                "depth": self.ddcscrdd_height - claim_height,
                "height": claim_height,
                "value": claim_value,
                "claim_sequence": claim_sequence,
                "address": claim_address
            }
            ddcscrdd_results = self.get_claims_for_name(claim_name)
            ddcscrdd_claim = None
            if ddcscrdd_results:
                for claim in ddcscrdd_results['claims']:
                    if claim['claimId'] == claim_id and claim['txid'] == claim_txid and claim['n'] == claim_nout:
                        ddcscrdd_claim = claim
                        break
                if ddcscrdd_claim:
                    result['supports'] = [[support['txid'], support['n'], support['nAmount']] for
                                          support in ddcscrdd_claim['supports']]
                    result['effective_amount'] = ddcscrdd_claim['nEffectiveAmount']
                    result['valid_at_height'] = ddcscrdd_claim['nValidAtHeight']

        return result

    def get_block(self, block_hash):
        block = self.ddcscrdd('getblock', (block_hash,))

        while True:
            try:
                response = [self.ddcscrdd("getrawtransaction", (txid,)) for txid in block['tx']]
            except:
                logger.error("ddcscrdd error (getfullblock)")
                self.wait_on_ddcscrdd()
                continue

            block['tx'] = response
            return block

    def catch_up(self, sync=True):
        self.start_catchup_height = self.storage.height
        prev_root_hash = None
        n = 0

        while not self.shared.stopped():
            # are we done yet?
            info = self.ddcscrdd('getinfo')
            self.relayfee = info.get('relayfee')
            self.ddcscrdd_height = info.get('blocks')
            ddcscrdd_block_hash = self.ddcscrdd('getblockhash', (self.ddcscrdd_height,))
            if self.storage.last_hash == ddcscrdd_block_hash:
                self.up_to_date = True
                break

            self.set_time()

            revert = (random.randint(1, 100) == 1) if self.test_reorgs and self.storage.height > 100 else False

            # not done..
            self.up_to_date = False
            try:
                next_block_hash = self.ddcscrdd('getblockhash', (self.storage.height + 1,))
            except BaseException, e:
                revert = True

            next_block = self.get_block(next_block_hash if not revert else self.storage.last_hash)

            if (next_block.get('previousblockhash') == self.storage.last_hash) and not revert:

                prev_root_hash = self.storage.get_root_hash()

                n = self.import_block(next_block, next_block_hash, self.storage.height + 1)
                self.storage.height = self.storage.height + 1
                self.write_header(self.block2header(next_block), sync)
                self.storage.last_hash = next_block_hash

            else:

                # revert current block
                block = self.get_block(self.storage.last_hash)
                print_log("blockchain reorg", self.storage.height, block.get('previousblockhash'),
                          self.storage.last_hash)
                n = self.import_block(block, self.storage.last_hash, self.storage.height, revert=True)
                self.pop_header()
                self.flush_headers()

                self.storage.height -= 1

                # read previous header from disk
                self.header = self.read_header(self.storage.height)
                self.storage.last_hash = self.hash_header(self.header)

                if prev_root_hash:
                    assert prev_root_hash == self.storage.get_root_hash()
                    prev_root_hash = None

            # print time
            self.print_time(n)

        self.header = self.block2header(self.ddcscrdd('getblock', (self.storage.last_hash,)))
        self.header['utxo_root'] = self.storage.get_root_hash().encode('hex')

        if self.shared.stopped():
            print_log("closing database")
            self.storage.close()

    def memorypool_update(self):
        t0 = time.time()
        mempool_hashes = set(self.ddcscrdd('getrawmempool'))
        touched_addresses = set()

        # get new transactions
        new_tx = {}
        for tx_hash in mempool_hashes:
            if tx_hash in self.mempool_hashes:
                continue

            tx = self.get_mempool_transaction(tx_hash)
            if not tx:
                continue

            new_tx[tx_hash] = tx

        # remove older entries from mempool_hashes
        self.mempool_hashes = mempool_hashes

        # check all tx outputs
        for tx_hash, tx in new_tx.iteritems():
            mpa = self.mempool_addresses.get(tx_hash, {})
            out_values = []
            for x in tx.get('outputs'):
                addr = x.get('address', '')
                out_values.append((addr, x['value']))
                if not addr:
                    continue
                v = mpa.get(addr, 0)
                v += x['value']
                mpa[addr] = v
                touched_addresses.add(addr)

            self.mempool_addresses[tx_hash] = mpa
            self.mempool_values[tx_hash] = out_values

        # check all inputs
        for tx_hash, tx in new_tx.iteritems():
            mpa = self.mempool_addresses.get(tx_hash, {})
            for x in tx.get('inputs'):
                mpv = self.mempool_values.get(x.get('prevout_hash'))
                if mpv:
                    addr, value = mpv[x.get('prevout_n')]
                else:
                    txi = (x.get('prevout_hash') + int_to_hex(x.get('prevout_n'), 4)).decode('hex')
                    try:
                        addr = self.storage.get_address(txi)
                        value = self.storage.get_utxo_value(addr, txi)
                    except:
                        print_log("utxo not in database; postponing mempool update")
                        return

                if not addr:
                    continue
                v = mpa.get(addr, 0)
                v -= value
                mpa[addr] = v
                touched_addresses.add(addr)

            self.mempool_addresses[tx_hash] = mpa

        # remove deprecated entries from mempool_addresses
        for tx_hash, addresses in self.mempool_addresses.items():
            if tx_hash not in self.mempool_hashes:
                self.mempool_addresses.pop(tx_hash)
                self.mempool_values.pop(tx_hash)
                touched_addresses.update(addresses)

        # remove deprecated entries from mempool_hist
        new_mempool_hist = {}
        for addr in self.mempool_hist.iterkeys():
            h = self.mempool_hist[addr]
            hh = []
            for tx_hash, delta in h:
                if tx_hash in self.mempool_addresses:
                    hh.append((tx_hash, delta))
            if hh:
                new_mempool_hist[addr] = hh
        # add new transactions to mempool_hist
        for tx_hash in new_tx.iterkeys():
            addresses = self.mempool_addresses[tx_hash]
            for addr, delta in addresses.iteritems():
                h = new_mempool_hist.get(addr, [])
                if (tx_hash, delta) not in h:
                    h.append((tx_hash, delta))
                new_mempool_hist[addr] = h

        with self.mempool_lock:
            self.mempool_hist = new_mempool_hist

        # invalidate cache for touched addresses
        for addr in touched_addresses:
            self.invalidate_cache(addr)

        t1 = time.time()
        if t1 - t0 > 1:
            print_log('mempool_update', t1 - t0, len(self.mempool_hashes), len(self.mempool_hist))

    def invalidate_cache(self, address):
        with self.cache_lock:
            if address in self.history_cache:
                # print_log("cache: invalidating", address)
                self.history_cache.pop(address)

        with self.watch_lock:
            sessions = self.watched_addresses.get(address)

        if sessions:
            # TODO: update cache here. if new value equals cached value, do not send notification
            self.address_queue.put((address, sessions))

    def close(self):
        self.blockchain_thread.join()
        print_log("Closing database...")
        self.storage.close()
        print_log("Database is closed")

    def main_iteration(self):
        if self.shared.stopped():
            print_log("Stopping timer")
            return

        self.catch_up()

        self.memorypool_update()

        if self.sent_height != self.storage.height:
            self.sent_height = self.storage.height
            for session in self.watch_blocks:
                self.push_response(session, {
                    'id': None,
                    'method': 'blockchain.numblocks.subscribe',
                    'params': (self.storage.height,),
                })

        if self.sent_header != self.header:
            self.sent_header = self.header
            for session in self.watch_headers:
                self.push_response(session, {
                    'id': None,
                    'method': 'blockchain.headers.subscribe',
                    'params': (self.header,),
                })

        while True:
            try:
                addr, sessions = self.address_queue.get(False)
            except:
                break

            status = self.get_status(addr)
            for session in sessions:
                self.push_response(session, {
                    'id': None,
                    'method': 'blockchain.address.subscribe',
                    'params': (addr, status),
                })

    def do_subscribe(self, method, params, session):
        with self.watch_lock:
            if method == 'blockchain.numblocks.subscribe':
                if session not in self.watch_blocks:
                    self.watch_blocks.append(session)

            elif method == 'blockchain.headers.subscribe':
                if session not in self.watch_headers:
                    self.watch_headers.append(session)

            elif method == 'blockchain.address.subscribe':
                address = params[0]
                l = self.watched_addresses.get(address)
                if l is None:
                    self.watched_addresses[address] = [session]
                elif session not in l:
                    l.append(session)

    def do_unsubscribe(self, method, params, session):
        with self.watch_lock:
            if method == 'blockchain.numblocks.subscribe':
                if session in self.watch_blocks:
                    self.watch_blocks.remove(session)
            elif method == 'blockchain.headers.subscribe':
                if session in self.watch_headers:
                    self.watch_headers.remove(session)
            elif method == "blockchain.address.subscribe":
                addr = params[0]
                l = self.watched_addresses.get(addr)
                if not l:
                    return
                if session in l:
                    l.remove(session)
                if session in l:
                    print_log("error rc!!")
                    self.shared.stop()
                if not l:
                    self.watched_addresses.pop(addr)

    def _get_command(self, method):
        for attr_name in dir(self):
            if attr_name.startswith("cmd_"):
                attr = getattr(self, attr_name)
                if hasattr(attr, "_is_command") and hasattr(attr, "_command_name"):
                    if attr._is_command and attr._command_name == method:
                        return attr
        raise BaseException("unknown method:%s" % method)

    def process(self, request, cache_only=False):
        message_id = request['id']
        # TODO: do something with message id

        fn = self._get_command(request['method'])
        params = request.get('params', ())
        return fn(*params)


class BlockchainSubscriptionProcessor(BlockchainProcessorBase):
    @command('blockchain.numblocks.subscribe')
    def cmd_numblocks_subscribe(self):
        return self.storage.height

    @command('blockchain.headers.subscribe')
    def cmd_headers_subscribe(self):
        return self.header

    @command('blockchain.address.subscribe')
    def cmd_address_subscribe(self, address, cache_only=False):
        address = str(address)
        return self.get_status(address, cache_only)


class BlockchainProcessor(BlockchainSubscriptionProcessor):
    @command('blockchain.address.get_history')
    def cmd_address_get_history(self, address, cache_only=False):
        address = str(address)
        return self.get_history(address, cache_only)

    @command('blockchain.address.get_mempool')
    def cmd_address_get_mempool(self, address):
        address = str(address)
        return self.get_unconfirmed_history(address)

    @command('blockchain.address.get_balance')
    def cmd_address_get_balance(self, address):
        address = str(address)
        confirmed = self.storage.get_balance(address)
        unconfirmed = self.get_unconfirmed_value(address)
        return {'confirmed': confirmed, 'unconfirmed': unconfirmed}

    @command('blockchain.address.get_proof')
    def cmd_address_get_proof(self, address):
        address = str(address)
        return self.storage.get_proof(address)

    @command('blockchain.address.listunspent')
    def cmd_address_list_unspent(self, address):
        address = str(address)
        return self.storage.listunspent(address)

    @command('blockchain.utxo.get_address')
    def cmd_utxo_get_address(self, txid, pos):
        txid = str(txid)
        pos = int(pos)
        txi = (txid + int_to_hex(pos, 4)).decode('hex')
        return self.storage.get_address(txi)

    @command('blockchain.block.get_header')
    def cmd_block_get_header(self, height, cache_only=False):
        height = int(height)
        if cache_only:
            result = -1
        else:
            result = self.get_header(height)
        return result

    @command('blockchain.block.get_chunk')
    def cmd_block_get_chunk(self, index, cache_only=False):
        index = int(index)
        if cache_only:
            result = -1
        else:
            result = self.get_chunk(index)
        return result

    @command('blockchain.transaction.broadcast')
    def cmd_transaction_broadcast(self, raw_transaction):
        raw_transaction = str(raw_transaction)
        try:
            txo = self.ddcscrdd('sendrawtransaction', (raw_transaction,))
            print_log("sent tx:", txo)
            result = txo
        except BaseException, e:
            error = e.args[0]
            if error["code"] == -26:
                # If we return anything that's not the transaction hash,
                #  it's considered an error message
                message = error["message"]
                result = "The transaction was rejected by network rules.(%s)\n[%s]" % (
                    message, raw_transaction)
            else:
                result = error["message"]  # do send an error
            print_log("error:", result)
        return result

    @command('blockchain.transaction.get_merkle')
    def cmd_transaction_get_merkle(self, tx_hash, height, cache_only=False):
        tx_hash = str(tx_hash)
        height = int(height)
        return self.get_merkle(tx_hash, height, cache_only)

    @command('blockchain.transaction.get_height')
    def cmd_transaction_get_height(self, tx_hash):
        tx_hash = str(tx_hash)
        transaction_info = self.ddcscrdd('getrawtransaction', (tx_hash, 1))
        if transaction_info and 'hex' in transaction_info and 'confirmations' in transaction_info:
            # an unconfirmed transaction from ddcscrdd will not have a 'confirmations' field
            height = self.ddcscrdd_height - transaction_info['confirmations']
            return height
        elif transaction_info and 'hex' in transaction_info:
            return -1
        return None

    @command('blockchain.transaction.get')
    def cmd_transaction_get(self, tx_hash, height=None):
        # height argument does nothing here but is used in ddcsum synchronizer
        tx_hash = str(tx_hash)
        return self.get_raw_transaction(tx_hash)

    @command('blockchain.estimatefee')
    def cmd_estimate_fee(self, num):
        num = int(num)
        return self.ddcscrdd('estimatefee', (num,))

    @command('blockchain.relayfee')
    def cmd_relay_fee(self):
        return self.relayfee

    @command('blockchain.claimtrie.getvalue')
    def cmd_claimtrie_getvalue(self, name, block_hash=None):
        name = str(name)
        if block_hash:
            proof = self.ddcscrdd('getnameproof', (name, block_hash))
        else:
            proof = self.ddcscrdd('getnameproof', (name,))

        result = {'proof': proof}
        if ddcscrd_proof_has_winning_claim(proof):
            txid, nout = str(proof['txhash']), int(proof['nOut'])
            transaction_info = self.ddcscrdd('getrawtransaction', (proof['txhash'], 1))
            transaction = transaction_info['hex']
            transaction_height = self.ddcscrdd_height - transaction_info['confirmations']
            result['transaction'] = transaction
            claim_id = self.storage.get_claim_id_from_outpoint(txid, nout)
            result['height'] = transaction_height + 1

        claim_info = self.get_claims_for_name(name)
        supports = []
        if len(claim_info['claims']) > 0:
            for claim in claim_info['claims']:
                if claim['txid'] == txid and claim['n'] == nout:
                    claim_id = claim['claimId']
                    result['claim_id'] = claim_id
                    claim_sequence = self.storage.get_n_for_name_and_claimid(str(name), claim_id)
                    result['claim_sequence'] = claim_sequence
                    supports = claim['supports']
                    break
        result['supports'] = [[support['txid'], support['n'], support['nAmount']] for support in
                              supports]
        return result

    @command('blockchain.claimtrie.getclaimsintx')
    def cmd_claimtrie_getclaimsintx(self, txid):
        txid = str(txid)
        result = self.ddcscrdd('getclaimsfortx', (txid,))
        if result:
            results_for_return = []
            for claim in result:
                claim_id = str(claim['claimId'])
                cached_claim = self.get_claim_info(claim_id)
                results_for_return.append(cached_claim)
            return results_for_return

    @command('blockchain.claimtrie.getclaimsforname')
    def cmd_claimtrie_getclaimsforname(self, name):
        name = str(name)
        result = self.get_claims_for_name(name)
        if result:
            claims = []
            for claim in result['claims']:
                claim_id = str(claim['claimId'])
                stored_claim = self.get_claim_info(claim_id)
                claims.append(stored_claim)
            result['claims'] = claims
            result['supports_without_claims'] = result['supports without claims']
            del result['supports without claims']
            result['last_takeover_height'] = result['nLastTakeoverHeight']
            del result['nLastTakeoverHeight']

        return result

    @command('blockchain.block.get_block')
    def cmd_get_block(self, block_hash):
        block_hash = str(block_hash)
        return self.ddcscrdd('getblock', (block_hash,))

    @command('blockchain.claimtrie.getclaimbyid')
    def cmd_claimtrie_getclaimbyid(self, claim_id):
        # TODO: add what proof is possible for claim id
        claim_id = str(claim_id)
        return self.get_claim_info(claim_id)

    @command('blockchain.claimtrie.getclaimsbyids')
    def cmd_batch_get_claims_by_id(self, *claim_ids):
        if len(claim_ids) > MAX_BATCH_URIS:
            raise Exception("Exceeds max batch uris of {}".format(MAX_BATCH_URIS))
        results = {}
        for claim_id in claim_ids:
            results[str(claim_id)] = self.get_claim_info(str(claim_id))
        return results

    @command('blockchain.claimtrie.getnthclaimforname')
    def cmd_claimtrie_getnthclaimforname(self, name, n):
        name = str(name)
        n = int(n)
        claim_id = str(self.storage.get_claimid_for_nth_claim_to_name(name, n))
        if claim_id:
            return self.get_claim_info(claim_id)

    @command('blockchain.claimtrie.getclaimssignedby')
    def cmd_claimtrie_getclaimssignedby(self, name):
        name = str(name)
        winning_claim = self.ddcscrdd('getvalueforname', (name,))
        if winning_claim:
            certificate_id = str(winning_claim['claimId'])
            claims = self.storage.get_claims_signed_by(certificate_id)
            return [self.get_claim_info(claim_id) for claim_id in claims]

    @command('blockchain.claimtrie.getclaimssignedbyid')
    def cmd_claimtrie_getclaimssignedbyid(self, certificate_id):
        certificate_id = str(certificate_id)
        if certificate_id:
            claims = self.storage.get_claims_signed_by(certificate_id)
            return [self.get_claim_info(claim_id) for claim_id in claims]

    @command('blockchain.claimtrie.getclaimssignedbynthtoname')
    def cmd_claimtrie_getclaimssignedbynthtoname(self, name, n):
        name = str(name)
        n = int(n)
        certificate_id = self.storage.get_claimid_for_nth_claim_to_name(name, n)
        if certificate_id:
            claims = self.storage.get_claims_signed_by(certificate_id)
            return [self.get_claim_info(claim_id) for claim_id in claims]

    def get_signed_claims_with_name_for_channel(self, channel_id, name):
        def iter_signed_by_with_name():
            for channel_claim in self.storage.get_claims_signed_by(channel_id):
                if self.storage.get_claim_name(channel_claim) == name:
                    yield channel_claim

        result = list(iter_signed_by_with_name())
        return result

    @command('blockchain.claimtrie.getvalueforuri')
    def cmd_claimtrie_get_value_for_uri(self, block_hash, uri):
        uri = str(uri)
        block_hash = str(block_hash)
        cache_key = block_hash + uri
        if cache_key in self.short_term_cache:
            return self.short_term_cache.get(cache_key)
        try:
            parsed_uri = parse_ddcs_uri(uri)
        except URIParseError as err:
            return {'error': err.message}
        result = {}

        if parsed_uri.is_channel:
            certificate = None
            if parsed_uri.claim_id:
                certificate_info = self.get_claim_info(parsed_uri.claim_id)
                if certificate_info and certificate_info['name'] == parsed_uri.name:
                    certificate = {'resolution_type': CLAIM_ID, 'result': certificate_info}
            elif parsed_uri.claim_sequence:
                claim_id = self.storage.get_claimid_for_nth_claim_to_name(str(parsed_uri.name),
                                                                          parsed_uri.claim_sequence)
                certificate_info = self.get_claim_info(str(claim_id))
                if certificate_info:
                    certificate = {'resolution_type': SEQUENCE, 'result': certificate_info}
            else:
                certificate_info = self.cmd_claimtrie_getvalue(parsed_uri.name, block_hash)
                if certificate_info:
                    certificate = {'resolution_type': WINNING, 'result': certificate_info}

            if certificate and not parsed_uri.path:
                result['certificate'] = certificate
                channel_id = certificate['result'].get('claim_id') or certificate['result'].get('claimId')
                channel_id = str(channel_id)
                claim_ids_in_channel = self.storage.get_claims_signed_by(channel_id)
                claims_in_channel = {cid: (self.storage.get_claim_name(cid),
                                           self.storage.get_claim_height(cid))
                                     for cid in claim_ids_in_channel}
                result['unverified_claims_in_channel'] = claims_in_channel
            elif certificate:
                result['certificate'] = certificate
                channel_id = certificate['result'].get('claim_id') or certificate['result'].get('claimId')
                channel_id = str(channel_id)
                claim_ids_matching_name = self.get_signed_claims_with_name_for_channel(channel_id, parsed_uri.path)

                claims_in_channel = {cid: (self.storage.get_claim_name(cid),
                                           self.storage.get_claim_height(cid))
                                     for cid in claim_ids_matching_name}
                result['unverified_claims_for_name'] = claims_in_channel
        else:
            claim = None
            if parsed_uri.claim_id:
                claim_info = self.get_claim_info(parsed_uri.claim_id)
                if claim_info and claim_info['name'] == parsed_uri.name:
                    claim = {'resolution_type': CLAIM_ID, 'result': claim_info}
            elif parsed_uri.claim_sequence:
                claim_id = self.storage.get_claimid_for_nth_claim_to_name(str(parsed_uri.name),
                                                                          parsed_uri.claim_sequence)
                claim_info = self.get_claim_info(str(claim_id))
                if claim_info:
                    claim = {'resolution_type': SEQUENCE, 'result': claim_info}
            else:
                claim_info = self.cmd_claimtrie_getvalue(parsed_uri.name, block_hash)
                if claim_info:
                    claim = {'resolution_type': WINNING, 'result': claim_info}
            if (claim and
                # is not an unclaimed winning name
                (claim['resolution_type'] != WINNING or ddcscrd_proof_has_winning_claim(claim['result']['proof']))):
                try:
                    claim_val = self.get_claim_info(claim['result']['claim_id'])
                    decoded = smart_decode(claim_val['value'])
                    if decoded.certificate_id:
                        certificate_info = self.get_claim_info(decoded.certificate_id)
                        if certificate_info:
                            certificate = {'resolution_type': CLAIM_ID,
                                           'result': certificate_info}
                            result['certificate'] = certificate
                except DecodeError:
                    pass
                result['claim'] = claim
        self.short_term_cache.put(cache_key, result)
        return result

    @command('blockchain.claimtrie.getvaluesforuris')
    def cmd_batch_claimtrie_get_value_for_uri(self, block_hash, *uris):
        if len(uris) > MAX_BATCH_URIS:
            raise Exception("Exceeds max batch uris of {}".format(MAX_BATCH_URIS))
        results = {}
        for uri in uris:
            results[uri] = self.cmd_claimtrie_get_value_for_uri(block_hash, uri)
        return results
