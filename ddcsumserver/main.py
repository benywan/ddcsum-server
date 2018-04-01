#!/usr/bin/env python
# Copyright(C) 2012 thomasv@gitorious

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/agpl.html>.

import argparse
import ConfigParser
import logging
import socket
import sys
import time
import json
import os
import xmlrpclib
import signal
import traceback
import ddcsschema

from ddcsumserver import storage, networks, utils
from ddcsumserver.processor import Dispatcher, print_log
from ddcsumserver.server_processor import ServerProcessor
from ddcsumserver.blockchain_processor import BlockchainProcessor
from ddcsumserver.stratum_tcp import TcpServer
from ddcsumserver.stratum_http import HttpServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

logging.basicConfig()

if sys.maxsize <= 2 ** 32:
    print "Warning: it looks like you are using a 32bit system. You may experience crashes caused by mmap"

if os.getuid() == 0:
    print "Do not run this program as root!"
    print "Run the install script to create a non-privileged user."
    sys.exit()


def parse_ddcscrdd(conf_lines):
    for line in conf_lines:
        if line.startswith("rpcuser="):
            yield "ddcscrdd_user", line[8:].rstrip('\n')
        elif line.startswith("rpcpassword="):
            yield "ddcscrdd_password", line[12:].rstrip('\n')
        elif line.startswith("rpcport="):
            yield "ddcscrdd_port", line[8:].rstrip('\n')


def load_ddcscrdd_connection_info(config, wallet_conf):
    type = config.get('network', 'type')
    params = networks.params.get(type)

    settings = {
        "ddcscrdd_user": "rpcuser",
        "ddcscrdd_password": "rpcpassword",
        "ddcscrdd_port": str(params.get('default_rpc_port')),
        "ddcscrdd_host": "localhost"
    }
    with open(wallet_conf, "r") as conf:
        conf_lines = conf.readlines()
    ddcscrdd_settings = {}
    for k, v in parse_ddcscrdd(conf_lines):
        ddcscrdd_settings.update({k: v})
    settings.update(ddcscrdd_settings)
    if not config.has_section('ddcscrdd'):
        config.add_section('ddcscrdd')
    for k, v in settings.iteritems():
        if not config.has_option('ddcscrdd', k):
            config.set('ddcscrdd', k, v)


def attempt_read_config(config, filename):
    try:
        with open(filename, 'r') as f:
            config.readfp(f)
    except IOError:
        pass


def setup_network_settings(config):
    network_type = config.get('network', 'type')
    params = networks.params.get(network_type)
    utils.PUBKEY_ADDRESS = int(params.get('pubkey_address'))
    utils.SCRIPT_ADDRESS = int(params.get('script_address'))
    utils.PUBKEY_ADDRESS_PREFIX = int(params.get('pubkey_address_prefix'))
    utils.SCRIPT_ADDRESS_PREFIX = int(params.get('script_address_prefix'))
    storage.GENESIS_HASH = params.get('genesis_hash')
    ddcsschema.BLOCKCHAIN_NAME = network_type


DEFAULT_DATA_DIR = os.path.join(os.path.expanduser("~/"), '.ddcsumserver')
DEFAULT_DDCSUM_LOG_DIR = DEFAULT_DATA_DIR

if not os.path.isdir(DEFAULT_DATA_DIR):
    os.mkdir(DEFAULT_DATA_DIR)
if not os.path.isdir(DEFAULT_DDCSUM_LOG_DIR):
    os.mkdir(DEFAULT_DDCSUM_LOG_DIR)


def create_config(filename=None, ddcscrdd_dir=None):
    config = ConfigParser.ConfigParser()
    # set some defaults, which will be overwritten by the config file
    config.add_section('server')
    config.set('server', 'banner', 'Welcome to ddcsum!')
    config.set('server', 'host', 'localhost')
    config.set('server', 'ddcsum_rpc_port', '8000')
    config.set('server', 'report_host', '')
    config.set('server', 'stratum_tcp_port', '50001')
    config.set('server', 'stratum_http_port', '')
    config.set('server', 'stratum_tcp_ssl_port', '50002')
    config.set('server', 'stratum_http_ssl_port', '')
    config.set('server', 'report_stratum_tcp_port', '')
    config.set('server', 'report_stratum_http_port', '')
    config.set('server', 'report_stratum_tcp_ssl_port', '')
    config.set('server', 'report_stratum_http_ssl_port', '')
    config.set('server', 'ssl_certfile', '')
    config.set('server', 'ssl_keyfile', '')
    config.set('server', 'coin', '')
    config.set('server', 'logfile', os.path.join(DEFAULT_DDCSUM_LOG_DIR, "ddcsum.log"))
    config.set('server', 'donation_address', '')
    config.set('server', 'max_subscriptions', '15000')

    config.add_section('leveldb')
    config.set('leveldb', 'path', os.path.join(DEFAULT_DATA_DIR, 'ddcsum_db'))
    config.set('leveldb', 'utxo_cache', str(64 * 1024 * 1024))
    config.set('leveldb', 'hist_cache', str(80 * 1024))
    config.set('leveldb', 'addr_cache', str(16 * 1024 * 1024))
    config.set('leveldb', 'claimid_cache', str(16 * 1024 * 1024 * 8))

    config.set('leveldb', 'claim_value_cache', str(1024 * 1024 * 1024))

    config.set('leveldb', 'profiler', 'no')

    # Beaker caching
    config.add_section('caching')
    config.set('caching', 'data_dir', os.path.join(DEFAULT_DATA_DIR, 'beaker'))
    config.set('caching', 'type', 'dbm')
    config.set('caching', 'short_expire', '60')
    config.set('caching', 'long_expire', '600')

    # set network parameters
    config.add_section('network')
    config.set('network', 'type', 'ddcscrd_main')

    # try to find the config file in the default paths
    if not filename:
        if sys.platform == "darwin":
            filename = os.path.join(os.path.expanduser("~/"), 'ddcsum.conf')
        else:
            for path in ('/etc/', ''):
                filename = path + 'ddcsum.conf'
                if os.path.isfile(filename):
                    break

    attempt_read_config(config, filename)

    if sys.platform == "darwin":
        default_ddcscrdd_dir = os.path.join(os.path.expanduser("~/"), "Library", "Application Support", "ddcscrd")
    else:
        default_ddcscrdd_dir = os.path.join(os.path.expanduser("~/"), ".ddcscrd")

    ddcscrdd_dir = ddcscrdd_dir or default_ddcscrdd_dir
    ddcscrdd_conf = os.path.join(ddcscrdd_dir, "ddcscrd.conf")
    if os.path.isfile(ddcscrdd_conf):
        print_log("loading ddcscrdd info")
        load_ddcscrdd_connection_info(config, ddcscrdd_conf)
        found_ddcscrdd = True
    else:
        print_log("no config for ddcscrdd found (%s)" % ddcscrdd_conf)
        found_ddcscrdd = False

    if not os.path.isfile(filename):
        print 'could not find ddcsum configuration file "%s"' % filename
        if not found_ddcscrdd:
            print "could not find ddcscrdd configutation file"
            sys.exit(1)

    return config


def run_rpc_command(params, ddcsum_rpc_port):
    cmd = params[0]
    server = xmlrpclib.ServerProxy('http://localhost:%d' % ddcsum_rpc_port)
    func = getattr(server, cmd)
    r = func(*params[1:])
    if cmd == 'sessions':
        now = time.time()
        print 'type           address         sub  version  time'
        for item in r:
            print '%4s   %21s   %3s  %7s  %.2f' % (item.get('name'),
                                                   item.get('address'),
                                                   item.get('subscriptions'),
                                                   item.get('version'),
                                                   (now - item.get('time')),
                                                   )
    elif cmd == 'debug':
        print r
    else:
        print json.dumps(r, indent=4, sort_keys=True)


def cmd_getinfo():
    return {
        'blocks': chain_proc.storage.height,
        'peers': len(server_proc.peers),
        'sessions': len(dispatcher.request_dispatcher.get_sessions()),
        'watched': len(chain_proc.watched_addresses),
        'cached': len(chain_proc.history_cache),
    }


def cmd_sessions():
    return map(lambda s: {"time": s.time,
                          "name": s.name,
                          "address": s.address,
                          "version": s.version,
                          "subscriptions": len(s.subscriptions)},
               dispatcher.request_dispatcher.get_sessions())


def cmd_numsessions():
    return len(dispatcher.request_dispatcher.get_sessions())


def cmd_peers():
    return server_proc.peers.keys()


def cmd_numpeers():
    return len(server_proc.peers)


def cmd_debug(s):

    if s:
        try:
            result = str(eval(s))
        except:
            err_lines = traceback.format_exc().splitlines()
            result = '%s | %s' % (err_lines[-3], err_lines[-1])
        return result


def get_port(config, name):
    try:
        return config.getint('server', name)
    except:
        return None


# share these as global, for 'debug' command
shared = None
chain_proc = None
server_proc = None
dispatcher = None
transports = []
tcp_server = None
ssl_server = None


def start_server(config):
    global shared, chain_proc, server_proc, dispatcher
    global tcp_server, ssl_server

    logfile = config.get('server', 'logfile')
    utils.init_logger(logfile)
    host = config.get('server', 'host')
    stratum_tcp_port = get_port(config, 'stratum_tcp_port')
    stratum_http_port = get_port(config, 'stratum_http_port')
    stratum_tcp_ssl_port = get_port(config, 'stratum_tcp_ssl_port')
    stratum_http_ssl_port = get_port(config, 'stratum_http_ssl_port')
    ssl_certfile = config.get('server', 'ssl_certfile')
    ssl_keyfile = config.get('server', 'ssl_keyfile')

    setup_network_settings(config)

    if ssl_certfile is '' or ssl_keyfile is '':
        stratum_tcp_ssl_port = None
        stratum_http_ssl_port = None

    print_log("Starting DDCSum server on", host)

    # Create hub
    dispatcher = Dispatcher(config)
    shared = dispatcher.shared

    # handle termination signals
    def handler(signum=None, frame=None):
        print_log('Signal handler called with signal', signum)
        shared.stop()

    for sig in [signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT]:
        signal.signal(sig, handler)

    # Create and register processors
    chain_proc = BlockchainProcessor(config, shared)
    dispatcher.register('blockchain', chain_proc)

    server_proc = ServerProcessor(config, shared)
    dispatcher.register('server', server_proc)

    # Create various transports we need
    if stratum_tcp_port:
        tcp_server = TcpServer(dispatcher, host, stratum_tcp_port, False, None, None)
        transports.append(tcp_server)

    if stratum_tcp_ssl_port:
        ssl_server = TcpServer(dispatcher, host, stratum_tcp_ssl_port, True, ssl_certfile, ssl_keyfile)
        transports.append(ssl_server)

    if stratum_http_port:
        http_server = HttpServer(dispatcher, host, stratum_http_port, False, None, None)
        transports.append(http_server)

    if stratum_http_ssl_port:
        https_server = HttpServer(dispatcher, host, stratum_http_ssl_port, True, ssl_certfile, ssl_keyfile)
        transports.append(https_server)

    for server in transports:
        server.start()

    return transports


def stop_server():
    shared.stop()
    server_proc.join()
    chain_proc.join()
    print_log("DDCSum Server stopped")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', metavar='path', default=None, help='specify a configuration file')
    parser.add_argument('command', nargs='*', default=[], help='send a command to the server')
    args = parser.parse_args()
    config = create_config(args.conf)

    ddcsum_rpc_port = get_port(config, 'ddcsum_rpc_port')

    if len(args.command) >= 1:
        try:
            run_rpc_command(args.command, ddcsum_rpc_port)
        except socket.error:
            print "server not running"
            sys.exit(1)
        sys.exit(0)

    try:
        run_rpc_command(['getpid'], ddcsum_rpc_port)
        is_running = True
    except socket.error:
        is_running = False

    if is_running:
        print "server already running"
        sys.exit(1)

    start_server(config)

    server = SimpleXMLRPCServer(('localhost', ddcsum_rpc_port), allow_none=True, logRequests=False)
    server.register_function(lambda: os.getpid(), 'getpid')
    server.register_function(shared.stop, 'stop')
    server.register_function(cmd_getinfo, 'getinfo')
    server.register_function(cmd_sessions, 'sessions')
    server.register_function(cmd_numsessions, 'numsessions')
    server.register_function(cmd_peers, 'peers')
    server.register_function(cmd_numpeers, 'numpeers')
    server.register_function(cmd_debug, 'debug')
    server.socket.settimeout(1)

    while not shared.stopped():
        try:
            server.handle_request()
        except socket.timeout:
            continue
        except:
            stop_server()

# Entrance
if __name__ == "__main__":
    main()
