Changelog
==========
[0.3.0] - UNRELEASED
--------------------

Added
~~~~~

- Metadata property for Reader and set_metadata for Writer to
  handle ORC file's metadata.
- Meta info fields like writer id, writer version for Reader.
- New TypeDescription subclasses to represent ORC types.

Changed
~~~~~~~

- Reimplemented TypeDescription in Python.

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