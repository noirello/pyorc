Changelog
==========
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
