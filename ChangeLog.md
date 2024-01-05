# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2024-01-05

- fixed support for `VST` communication protocol (DE-734, #31)
- fixed support for `VPACK` content type (DE-734, #31)
- updated ArangoDB Java Driver to version `7.4` (DE-734, #31)
- use the shaded variant of ArangoDB Java Driver (DE-734, #31)

## [1.1.0] - 2023-11-24

- added periodical connections rebalancing (#27)
- added Kafka 3.6 support (#25)
- added batch writes support (DE-627, #23)
- updated ArangoDB Java Driver to version `7.3`

## [1.0.0] - 2023-08-18

- initial release

[unreleased]: https://github.com/arangodb/kafka-connect-arangodb/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/arangodb/kafka-connect-arangodb/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/arangodb/kafka-connect-arangodb/compare/v1.0.0...v1.1.0
