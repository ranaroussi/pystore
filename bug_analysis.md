# PyStore Open Issues Analysis - Bugs and Technical Problems

## Summary of Unique Bugs and Technical Issues

### 1. **Append Function Data Loss/Failures**
- **Issues**: #43, #48, #65, #69, #26, #21, #53, #31, #38, #3, #9
- **Problem**: The `append()` function has multiple critical issues:
  - Silently loses data due to `drop_duplicates()` not considering the index
  - Drops rows with duplicate values even if they have different timestamps
  - Fails silently on macOS and other platforms
  - Issues with timezone-aware timestamps
  - String data appending causes data corruption

### 2. **TypeError with PyArrow**
- **Issue**: #75
- **Problem**: `TypeError: __cinit__() got an unexpected keyword argument 'times'` when using pyarrow

### 3. **Path Handling Issues**
- **Issues**: #72, #68, #27
- **Problem**: 
  - `utils.set_path` fails when argument is type `pathlib.Path`
  - Strange path behavior in IPython terminal in Spyder
  - Windows path issues with `WindowsPath` objects

### 4. **Metadata and Data Integrity Issues**
- **Issues**: #64, #62
- **Problem**:
  - `_updated` in metadata uses hour instead of minute
  - Reading items with metadata.json but no "_metadata" causes issues

### 5. **Performance and Memory Issues**
- **Issues**: #56, #22
- **Problem**: 
  - Append loads entire data into memory just to append new data
  - Terrible performance with dask 2.2.0 (orders of magnitude slower)

### 6. **Data Type and Compatibility Issues**
- **Issues**: #51, #39, #35, #25, #10, #6
- **Problem**:
  - Nested DataFrames cause exceptions
  - Writing pandas.Series fails
  - Cannot compare tz-naive and tz-aware timestamps
  - Nanosecond precision loss
  - Integer index converted to datetime automatically
  - Save and load data inconsistency

### 7. **Dependency and Installation Issues**
- **Issues**: #42, #46, #8, #5
- **Problem**:
  - Python-snappy installation fails on Windows
  - Dependabot authentication issues
  - macOS installation troubles
  - AttributeError with PosixPath objects

### 8. **Dask Version Compatibility**
- **Issues**: #24, #21, #22, #26, #17
- **Problem**:
  - Dask 2.2.0+ causes append to fail silently
  - Dask 2.3.0 compatibility issues
  - Bad escape errors with newer Dask versions
  - Multiple parquet files created instead of consolidating

### 9. **Parquet and File Management Issues**
- **Issues**: #17, #23, #31, #36
- **Problem**:
  - Append creates new parquet file for each operation instead of consolidating
  - npartitions parameter not passed correctly in append
  - Issues with partition size management

### 10. **Error Handling and Exceptions**
- **Issues**: #28, #16, #15
- **Problem**:
  - PermissionError when accessing item before creation
  - Unable to set npartitions when writing collection
  - Errors not properly surfaced in append operations

### 11. **Data Update Limitations**
- **Issue**: #32
- **Problem**: No way to update existing data (e.g., updating incomplete bars)

### 12. **Symbol and Special Character Issues**
- **Issue**: #45
- **Problem**: Symbols containing regex characters (like ^GSPC) are broken with dask/fastparquet/fsspec

### 13. **Tutorial and Documentation Issues**
- **Issues**: #60, #34, #13
- **Problem**:
  - Tutorial loading data problems
  - Missing information about duplicate handling in append
  - `.to_pandas()` requirement not clear in tutorial

### 14. **Multi-threading and Concurrent Access**
- **Issue**: #49
- **Problem**: How to read all columns except the partition column

### 15. **Platform-Specific Issues**
- **Issues**: #4, #27, #42
- **Problem**: 
  - Python 2.7 compatibility issues
  - Windows-specific path handling problems
  - Platform-specific installation challenges

## Critical Issues Summary

The most critical and recurring issues are:

1. **Data Loss in Append Operations** - This is by far the most serious issue affecting data integrity
2. **Dask Version Incompatibilities** - Breaking changes with newer Dask versions
3. **Performance Problems** - Memory usage and speed issues, especially with append
4. **Path and Platform Compatibility** - Cross-platform issues, especially on Windows
5. **Silent Failures** - Operations failing without proper error messages

These issues significantly impact the reliability and usability of PyStore for production use cases.