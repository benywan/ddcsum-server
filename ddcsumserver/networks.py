"""
Main network and testnet3 definitions
"""

params = {
    'ddcscrd_main': {
        'pubkey_address': 0,
        'script_address': 5,
        'pubkey_address_prefix': 85,
        'script_address_prefix': 122,
        'genesis_hash': '9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463',
        'default_rpc_port': 9245
    },
    'ddcscrd_test': {
        'pubkey_address': 0,
        'script_address': 5,
        'pubkey_address_prefix': 111,
        'script_address_prefix': 196,
        'genesis_hash': '9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463',
        'default_rpc_port': 19245
    },
    'ddcscrd_regtest': {
        'pubkey_address': 0,
        'script_address': 5,
        'pubkey_address_prefix': 111,
        'script_address_prefix': 196,
        'genesis_hash': '6e3fcf1299d4ec5d79c3a4c91d624a4acf9e2e173d95a1a0504f677669687556',
        'default_rpc_port': 18332
    }
}
