# Change Log
All notable changes to this project are documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### Fixed
- Fix `queue/initialize!` that fails to create MySQL tables due to index conflict.
- Internal: Fix possible test failures in non-UTC timezones.
- Internal: Fix graceful shutdown test.

### Changed
- Use JVM time when querying for pending jobs instead of relying on UTC.
- Verify the library works properly on Java 8.

