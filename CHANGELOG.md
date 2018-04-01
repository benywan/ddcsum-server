# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).
This project was forked from Electrum v2.7.1 thus the first release is
labeled as 2.7.1. Subsequent releases will follow
[Semantic Versioning](http://semver.org/).

## [Unreleased]
### Security
  *
  *

### Fixed
  *
  *

### Deprecated
  *
  *

### Changed
  *
  *

### Added
  *
  *

### Removed
  *
  *


## [1.0.20] - 2018-03-20
### Changed
 * Bumped `lbryschema` requirement to 0.0.15 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0015---2018-03-20)
 * internal `get_claims_for_name` and `get_raw_transaction` methods to use beaker caching
 * `blockchain.claimtrie.getvalueforuri` and `blockchain.claimtrie.getclaimsforname` to use beaker caching

### Removed
 * `blockchain.claimtrie.get` command


## [1.0.19] - 2018-03-02
### Changed
 * Updated dependency for leveldb

### Added
 * Added `blockchain.transaction.get_height` command


## [1.0.18] - 2018-01-28
### Fixed
 * Fixed address histories taking 10 seconds to update
 * Fixed bug handling uris for a claim specified by `claim_id` where the name does not match the name in the uri [#43](https://github.com/lbryio/lbryum-server/issues/43).


## [1.0.17] - 2017-11-08
### Changed
 * Bumped `lbryschema` requirement to 0.0.14 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0014---2017-11-08)

## [1.0.16] - 2017-10-25
### Changed
 * Bumped `lbryschema` requirement to 0.0.13 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0013---2017-10-25)
 * Bump `jsonschema` requirement to 2.6.0


## [1.0.15] - 2017-10-12
### Changed
 * Bumped `lbryschema` requirement to 0.0.12 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0012---2017-10-12)

## [1.0.14] - 2017-09-18
### Changed
 * Bumped `lbryschema` requirement to 0.0.11 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0011---2017-09-18)

## [1.0.13] - 2017-08-22
### Changed
 * Bumped `lbryschema` requirement to 0.0.10 [see changelog](https://github.com/lbryio/lbryschema/blob/master/CHANGELOG.md#0010---2017-08-22)

## [1.0.12] - 2017-07-24
### Added
 * Added changelog


