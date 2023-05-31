# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.2] - 2023-05-30

### Added

- Added missing typespec for `MongoQueue.ack/3` and `MongoQueue.nack/3`

### Fixed

- Fixed `:ok` response when the ack update count doesn’t match the number of acks submitted

## [0.0.1] - 2023-05-30

### Added

- Initial implementation
