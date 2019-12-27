Changelog
==========
[0.2.0] - UNRELEASED
--------------------

Added
~~~~~

- Converters for date, decimal and timestamp ORC types in Python and
  option to change them via Reader's and Writer's converters parameter.

Changed
~~~~~~~

- Use timezone-aware datetime objects (in UTC) for ORC timestamps by default.


[0.1.0] - 2019-11-16
--------------------

Added
~~~~~

- A Reader object to read ORC files.
- A stripe object to read only a stripe in an ORC file.
- A Writer object to write ORC files.
- A typedescription object to represent the ORC schema.
- Support to represent a struct type either a Python tuple or a dictionary.