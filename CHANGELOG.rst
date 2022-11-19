Changelog
==========
[0.8.0] - 2022-11-19
--------------------

Added
~~~~~

- Python 3.11 wheels. (PR #58, contribution of @dbaxa)

Changed
~~~~~~~

- ORC C++ Core updated to 1.7.7.
- Improved type annotations, set module's __all__ variable.


[0.7.0] - 2022-07-16
--------------------

Added
~~~~~

- Universal2 wheels for MacOS. (PR #55, contribution of @dbaxa)
- ORC-517, ORC-203, and ORC-14 versions to WriterVersion enum.

Changed
~~~~~~~

- Dropped support for Python 3.6.
- ORC C++ Core updated to 1.7.5.


[0.6.0] - 2022-02-18
--------------------

Added
~~~~~

- New parameter to Writer: dict_key_size_threshold for setting threshold
  for dictionary encoding. (PR #46, contribution of @dirtysalt)
- New parameter to Writer: padding_tolerance for block padding.
- New parameter to Reader and Writer: null_value for changing representation
  of ORC null value. The value must be a singleton object.
- Type stubs for classes implemented in C++.
- Experimental musllinux and PyPy wheels.

Changed
~~~~~~~

- Writer.writerows method reimplemented in C++.
- Improved type annotations.
- ORC C++ Core updated to 1.7.3.
- Removed build_orc setup.py command, moved the same functionality to
  build_ext command.

Fixed
~~~~~

- Unnecessary string casting of values when writing user metadata. (Issue #45)


[0.5.0] - 2021-10-22
--------------------

Added
~~~~~

- Module level variables for the ORC library version: orc_version string and
  orc_version_info namedtuple.
- New parameter for Writer: row_index_stride.
- New read-only properties for Reader: row_index_stride and software_version.
- Trino and Scritchley writer ids.
- Type annotations support for ORC types.
- Support for `timestamp with local time zone` type.
- New parameter for Reader and Writer: timezone.
- The backported zoneinfo module dependency pior to Python 3.9.
- Predicate (SearchArgument) support for filtering row groups during ORC file
  reads. New classes: Predicate and PredicateColumn.
- New parameter for Reader: predicate.
- Build for aarch64 wheels. (PR #43, contribution of @odidev)

Changed
~~~~~~~

- ORC C++ Core updated to 1.7.0, and because many of the new features are not
  backported to the 1.6 branch, currently this is the minimum required lib
  version.
- TimestampConverter's to_orc and from_orc methods got an extra timezone
  parameter, that will be bound to the same ZoneInfo object passed to the
  Reader or Writer via their timezone parameters during type convert.
- Renamed Reader.metadata property and Writer.set_metadata method to
  user_metadata and set_user_metadata respectively to avoid confusion.


[0.4.0] - 2021-01-11
--------------------

Added
~~~~~

- Experimental Windows support.
- tzdata package dependency on Windows. Automatically setting TZDIR
  to the path of the tzdata package's data dir after importing PyORC.

Changed
~~~~~~~
- Create ORC Type from TypeDescription directly (instead of string parsing)
  for Writer. (PR #26, contribution of @blkerby)
- Dotted column names are allowed to use in TypeDescription.find_column_id
  method with escaping them backticks.
- ORC C++ Core updated to 1.6.6.

Fixed
~~~~~

- Handling large negative seconds on Windows for TimestampConverter.from_orc.


[0.3.0] - 2020-05-24
--------------------

Added
~~~~~

- Metadata property for Reader and set_metadata for Writer to
  handle ORC file's metadata.
- Meta info attributes like writer_id, writer_version, bytes_length,
  compression and compression_block_size for Reader.
- New TypeDescription subclasses to represent ORC types.

Changed
~~~~~~~

- Reimplemented TypeDescription in Python.
- ORC C++ Core updated to 1.6.3.

Fixed
~~~~~

- Converting date from ORC on systems where the system's timezone
  has a negative UTC offset (Issues #5)


[0.2.0] - 2020-01-01
--------------------

Added
~~~~~

- Converters for date, decimal and timestamp ORC types in Python and
  option to change them via Reader's and Writer's converters parameter.
- Column object for accessing statistics about ORC columns.
- An attribute to Reader for selected schema.

Changed
~~~~~~~

- Use timezone-aware datetime objects (in UTC) for ORC timestamps by default.
- Wrapped C++ stripe object to Python Stripe.

Fixed
~~~~~

- Decrementing reference for bytes object after reading from file stream.

[0.1.0] - 2019-11-16
--------------------

Added
~~~~~

- A Reader object to read ORC files.
- A stripe object to read only a stripe in an ORC file.
- A Writer object to write ORC files.
- A typedescription object to represent the ORC schema.
- Support to represent a struct type either a Python tuple or a dictionary.
