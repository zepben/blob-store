# Release History
## [2.1.0] - UNRELEASED
### Breaking Changes
* None.

### New Features
* None.

### Enhancements
* None.

### Fixes
* None.

### Notes
* None.


## [2.0.0] - 2024-02-21

### Breaking Changes
* Updated the super pom with potentially breaking transitive dependencies.
* Converted to codebase to Kotlin, which will change the names of several accessors if interfacing from Java.
* Converted several functional interfaces to Kotlin `typealias` directives, which will impact Java use.

### New Features
* None.

### Enhancements
* None.

### Fixes

* Fixed several resource leaks when checking the schema of the blob store.

### Notes
* None.

## [1.3.1]

### Notes

* Revert 1.3.0 to 1.2.0
* Remove statement caching in SqliteBlobReader

## [1.3.0]

### Enhancements

* Make blob store thread safe
* Add db connection pool

## [1.2.0]

Initial open source release of blob-store.
