API documentation
*****************

.. automodule:: pyorc

:class:`Column`
===============

.. class:: Column(stream, index)

    An object that represents a column in an ORC file. It contains
    statistics about the column. If the `stream` is a :class:`Reader`
    object then the column refers to the entire ORC file, if its a 
    :class:`Stripe` then just the specified ORC stripe.

    :param Reader|Stripe stream: an ORC stream object (:class:`Reader`
        or :class:`Stripe`).
    :param int index: the index of the column.

.. attribute:: Column.statistics

    A dictionary object about the Column's statistics. It always contains
    the kind of the column, the number of values that does not include null
    values and a boolean value about either containing null values or not.
    It may contain other information depending on the kind of the column
    like minimum and maximum values, sums etc.


:class:`ORCConverter`
=====================

.. class:: ORCConverter()

    An abstract class for implementing own converters for `date`, `decimal`
    and `timestamp` types. These types are stored as integers in the ORC file
    and can be transformed into more convenient Python objects.

    The converter can be set to a :class:`Reader` or :class:`Writer` with
    the converters parameter, as a dictionary where the key is
    one of :attr:`TypeKind.DATE`, :attr:`TypeKind.DECIMAL`, or
    :attr:`TypeKind.TIMESTAMP`, and the value is the converter itself.

.. staticmethod:: ORCConverter.from_orc(*args)

    Builds high-level objects from basic ORC type. Its arguments depend on
    what ORC type the converter is bound:

        * `date`: the number of days since the epoch as a single integer.
        * `decimal`: the decimal number formatted as a string.
        * `timestamp`: seconds and nanoseconds since the epoch as integers
            and the ZoneInfo object passed to the Reader as timezone.

    :return: the constructed Python object.

.. staticmethod:: ORCConverter.to_orc(*args)

    Converts the high-level Python object to basic ORC type. Its arguments
    is a single Python object when the convert is bound to `date` or
    `timestamp`. The precision and scale are also passed to this method
    as integers, along with the object when it's bound to a decimal type,
    and the Writer's timezone as a ZoneInfo object when it's bound to a
    timestamp type.

    Expected return value:

        * `date`: the number of days since the epoch as a single integer.
        * `decimal`: an integer adjusted to the set precision and scale.
        * `timestamp`: a tuple of seconds and nanoseconds since the epoch
          as integers.


:class:`Predicate`
==================

.. class:: Predicate(operator, left, right)

    An object that represents an expression for filtering row groups in an ORC
    file. The supported operators are NOT, AND and OR, while the possible
    operands can be a :class:`PredicateColumn` or another Predicate. A Predicate
    is built from a :class:`PredicateColumn`, a literal value and a relation
    between the two.

    :param Operator operator: an operator type.
    :param Predicate|PredicateColumn left: the left operand.
    :param Predicate|PredicateColumn right: the right operand.

.. method:: Predicate.__or__(other)

    Set logical OR connection between to predicate expressions.

    :param Predicate other: the other predicate.

.. method:: Predicate.__and__(other)

    Set logical AND connection between to predicate expressions.

    :param Predicate other: the other predicate.

.. method:: Predicate.__invert__(other)

    Set logical NOT to a predicate expression.

    :param Predicate other: the other predicate.


:class:`PredicateColumn`
========================

.. class:: PredicateColumn(type_kind, name=None, index=None, precision=None,
                           scale=None)

    An object that represents a specific column to use in a predicate
    expression. It can be compared to literal value to create a
    :class:`Predicate`. A column can be addressed by either its name or its
    index.

    A simple predicate example, that filtering row groups where the
    ``col0`` column is less than 0:

    >>> pred = PredicateColumn("col0", TypeKind.INT) < 0)

    :param str name: the name of the column in the ORC file.
    :param TypeKind type_kind: the type of the column.
    :param int precision: the precision if the column's type is decimal.
    :param int scale: the scale if the column's type is decimal.

.. method:: PredicateColumn.__eq__(other)
.. method:: PredicateColumn.__ne__(other)
.. method:: PredicateColumn.__lt__(other)
.. method:: PredicateColumn.__le__(other)
.. method:: PredicateColumn.__gt__(other)
.. method:: PredicateColumn.__ge__(other)

    Simple comparison methods to compare a column and a literal value,
    and return a :class:`Predicate` object.

    :param other: a literal value for comparison.


:class:`Reader`
===============

.. class:: Reader(fileo, batch_size=1024, column_indices=None, \
                  column_names=None, timezone=zoneinfo.ZoneInfo("UTC"), \
                  struct_repr=StructRepr.TUPLE, converters=None, \
                  predicate=None, null_value=None)

    An object to read ORC files. The `fileo` must be a binary stream that
    support seeking. Either `column_indices` or `column_names` can be used
    to select specific columns from the ORC file.

    The object iterates over rows by calling :meth:`Reader.__next__`. By
    default the ORC struct type represented as a tuple, but it can be change
    by changing `struct_repr` to a valid :class:`StructRepr` value.

    For decimal, date and timestamp ORC types the default converters to
    Python objects can be change by setting a dictionary to the `converters`
    parameter. The dictionary's keys must be a :class:`TypeKind` and the
    values must implement the :class:`ORCConverter` abstract class.

    :param object fileo: a readable binary file-like object.
    :param int batch_size: The size of a batch to read.
    :param list column_indices: a list of column indices to read.
    :param list column_names: a list of column names to read.
    :param ZoneInfo timezone: a ZoneInfo object to use for parsing timestamp
        columns.
    :param StructRepr struct_repr: An enum to set the representation for
        an ORC struct type.
    :param dict converters: a dictionary, where the keys are
        :class:`TypeKind` and the values are subclasses of
        :class:`ORCConverter`.
    :param Predicate predicate: a predicate expression to read only specified
        row groups.
    :param object null_value: a singleton object to represent ORC null value.

.. method:: Reader.__getitem__(col_idx)

    Get a :class:`Column` object. The indexing is the same as it's in the
    ORC file which means `0` is the top-level, the first field in the
    top-level struct is `1`, if the `nth` field in the struct is a map then
    the `(n+1)th` index is the column of the keys and the `(n+2)th` index is
    the values, etc.

.. method:: Reader.__len__()

    Get the number of rows in the file.

.. method:: Reader.__next__()

    Get the next row from the file.

.. method:: Reader.iter_stripes()

    Get an iterator with the :class:`Stripe` objects from the file.

    :return: an iterator of :class:`Stripe` objects.
    :rtype: iterator

.. method:: Reader.read(rows=-1)

    Read the rows into memory. If `rows` is specified, at most number of
    rows will be read.

    :return: A list of rows.
    :rtype: list

.. method:: Reader.read_stripe(idx)

    Read a specific :class:`Stripe` object at `idx` from the ORC file.

    :param int idx: the index of the stripe.

    :return: a :class:`Stripe` object.
    :rtype: Stripe

.. method:: Reader.seek(row, whence=0)

    Jump to a certain row position in the file. Values for `whence` are:
        * 0 – start of the file (the default); offset should be zero or positive.
        * 1 – current file position; offset may be negative.
        * 2 – end of the file; offset should be negative.

    :return: number of the absolute row position.
    :rtype: int

.. attribute:: Reader.bytes_lengths

    The size information of the opened ORC file in bytes returned as a
    read-only dictionary. It includes the total file size (`file_length`),
    the length of the data stripes (`content_length`), the file footer
    (`file_footer_length`), postscript (`file_postscript_length`) and the
    stripe statistics (`stripe_statistics_length`).

    >>> example = open("deps/examples/demo-11-zlib.orc", "rb")
    >>> reader = pyorc.Reader(example)
    >>> reader.bytes_lengths
    {'content_length': 396823, 'file_footer_length': 2476, 'file_postscript_length': 25, 'file_length': 408522, 'stripe_statistics_length': 9197}

.. attribute:: Reader.compression

    Read-only attribute of the used compression of the file returned as
    a :class:`CompressionKind`.

.. attribute:: Reader.compression_block_size

    Read-only attribute of compression block size.

.. attribute:: Reader.current_row

    The current row position.

.. attribute:: Reader.format_version

    The Hive format version of the ORC file, represented as a tuple of
    `(MAJOR, MINOR)` versions.

    >>> reader.format_version
    (0, 11)

.. attribute:: Reader.user_metadata

    The user metadata information of the ORC file in a dictionary. The
    values are always bytes.

.. attribute:: Reader.num_of_stripes

    The number of stripes in the ORC file.

.. attribute:: Reader.row_index_stride

    The size of row index stride in the ORC file.

.. attribute:: Reader.schema

    A :class:`TypeDescription` object of the ORC file's schema. Always
    represents the full schema of the file, regardless which columns
    are selected to read.

.. attribute:: Reader.selected_schema

    A :class:`TypeDescription` object of the ORC file's schema that only
    represents the selected columns. If no columns are specified then it's
    the same as :attr:`Reader.schema`.

.. attribute:: Reader.software_version

    The version of the writer that created the ORC file, including the
    used implementation as well.

    >>> reader.software_version
    'ORC C++ 1.7.0'

.. attribute:: Reader.writer_id

    The identification of the writer that created the ORC file. The known
    writers are the official Java writer, the C++ writer and the Presto writer.
    Other possible writers are represented as ``"UNKNOWN_WRITER"``.

    >>> reader.writer_id
    'ORC_JAVA_WRITER'

.. attribute:: Reader.writer_version

    The version of the writer created the file, returned as
    :class:`WriterVersion`. This version is used to mark significant changes
    (that doesn't change the file format) and helps the reader to handle
    the corresponding file correctly.


:class:`Stripe`
===============

.. class:: Stripe(reader, idx)

    An object that represents a stripe in an ORC file. It's iterable just
    like :class:`Reader`, and inherits many of its methods, but the read
    rows are limited to the stripe.

    :param Reader reader: a reader object.
    :param int idx: the index of the stripe.

.. method:: Stripe.__getitem__(col_idx)

    Get a :class:`Column` object, just like :meth:`Reader.__getitem__`, but
    only for the current stripe.

.. method:: Stripe.__len__()

    Get the number of rows in the stripe.

.. method:: Stripe.__next__()

    Get the next row from the stripe.

.. method:: Stripe.seek(row, whence=0)

    Jump to a certain row position in the stripe. For possible `whence`
    values see :meth:`Reader.seek`.

    :return: number of the absolute row position in the stripe.
    :rtype: int

.. method:: Stripe.read(rows=-1)

    Read the rows into memory. If `rows` is specified, at most number of
    rows will be read.

    :return: A list of rows.
    :rtype: list

.. attribute:: Stripe.bloom_filter_columns

    The list of column indices that have Bloom filter.

.. attribute:: Stripe.bytes_length

    The length of the stripe in bytes.

.. attribute:: Stripe.bytes_offset

    The bytes offset where the stripes starts in the file.

.. attribute:: Stripe.current_row

    The current row position in the stripe.

.. attribute:: Stripe.row_offset

    The row offset where the stripes starts in the file.

.. attribute:: Stripe.writer_timezone

    The timezone information of the writer.


:class:`TypeDescription`
========================

.. class:: TypeDescription()

    The base class for representing a type of an ORC schema. A schema
    consists one or more instances that are inherited from the
    TypeDescription class.

.. staticmethod:: TypeDescription.from_string(schema)

    Returns instances of TypeDescription objects from a string
    representation of an ORC schema.

.. method:: TypeDescription.find_column_id(name)

    Find the its id of a column by its name.

.. method:: TypeDescription.set_attributes(attrs)

    Annotate the ORC type with custom attributes. The `attrs` parameter
    must be a dictionary with string keys and string values.

.. attribute:: TypeDescription.attributes

    Return the attributes that the column is annotated with.

.. attribute:: TypeDescription.column_id

    The id of the column.

.. attribute:: TypeDescription.kind

    The kind of the current TypeDescription instance. It has to be one of
    the :class:`pyorc.TypeKind` enum values.

.. class:: Boolean()

    Class for representing `boolean` ORC type.

.. class:: TinyInt()

    Class for representing `tinyint` ORC type.

.. class:: SmallInt()

    Class for representing `smallint` ORC type.

.. class:: Int()

    Class for representing `int` ORC type.

.. class:: BigInt()

    Class for representing `bigint` ORC type.

.. class:: SmallInt()

    Class for representing `smallint` ORC type.

.. class:: Float()

    Class for representing `float` ORC type.

.. class:: Double()

    Class for representing `double` ORC type.

.. class:: String()

    Class for representing `string` ORC type.

.. class:: Binary()

    Class for representing `binary` ORC type.

.. class:: Timestamp()

    Class for representing `timestamp` ORC type.

.. class:: TimestampInstant()

    Class for representing `timestamp with local time zone` ORC type.

.. class:: Date()

    Class for representing `date` ORC type.

.. class:: Char(max_length)

    Class for representing `char` ORC type with the parameter of the
    length of the character sequence.

    :param int max_length: the maximal length of the character sequence.

.. class:: VarChar(max_length)

    Class for representing `varchar` ORC type with the parameter of the
    maximal length of the variable character sequence.

    :param int max_length: the maximal length of the character sequence.

.. class:: Decimal(precision, scale)

    Class for representing `decimal` ORC type with the parameters of
    precision and scale.

    :param int precision: the precision of the decimal number.
    :param int scale: the scale of the decimal number.

.. class:: Union(*cont_types)

    Class for representing `uniontype` ORC compound type. Its arguments must
    be TypeDescription instances for the possible type variants.

    :param TypeDescription \*cont_types: the list of TypeDescription
        instances for the possible type variants.

.. class:: Array(cont_type)

    Class for representing `array` ORC compound type with the parameter
    of the contained ORC type.

    :param TypeDescription cont_type: the instance of the contained type.

.. class:: Map(key, value)

    Class for representing `map` ORC compound type with parameters for the
    key and value ORC types.

    :param TypeDescription key: the instance type of the key in the map.
    :param TypeDescription value: the instance type of the value in the map.

.. class:: Struct(**fields)

    Class for representing `struct` ORC compound type with keyword arguments
    of its fields. The fields must be TypeDescription instances.

    >>> schema = Struct(
    ...    field0=Int(),
    ...    field1=Map(key=String(),value=Double()),
    ...    field2=Timestamp(),
    ... )
    >>> str(schema)
    "struct<field0:int,field1:map<string,double>,field2:timestamp>"

    :param TypeDescription \**fields: the keywords of TypeDescription
        instances for the possible fields in the struct.


:class:`Writer`
===============

.. class:: Writer(fileo, schema, batch_size=1024, \
                  stripe_size=67108864, row_index_stride=10000, \
                  compression=CompressionKind.ZLIB, \
                  compression_strategy=CompressionStrategy.SPEED, \
                  compression_block_size=65536, bloom_filter_columns=None, \
                  bloom_filter_fpp=0.05, timezone=zoneinfo.ZoneInfo("UTC"), \
                  struct_repr=StructRepr.TUPLE, converters=None, \
                  padding_tolerance=0.0, dict_key_size_threshold=0.0, \
                  null_value=None)

    An object to write ORC files. The `fileo` must be a binary stream.
    The `schema` must be :class:`TypeDescription` or a valid ORC schema
    definition as a string.

    With the `bloom_filter_columns` a list of column ids or field names
    can be set to create a Bloom filter for the column. Nested structure
    fields can be selected with dotted format. For example in a file with
    a ``struct<first:struct<second:int>>`` schema the second column can be
    selected as ``["first.second"]``.

    For decimal, date and timestamp ORC types the default converters from
    Python objects can be change by setting a dictionary to the `converters`
    parameter. The dictionary's keys must be a :class:`TypeKind` and the
    values must implement the :class:`ORCConverter` abstract class.

    :param object fileo: a writeable binary file-like object.
    :param TypeDescription|str schema: the ORC schema of the file.
    :param int batch_size: the batch size for the ORC file.
    :param int stripe_size: the stripes size in bytes.
    :param int row_index_stride: the size of the row index stride.
    :param CompressionKind compression: the compression kind for the ORC
        file.
    :param CompressionStrategy compression_strategy: the compression
        strategy.
    :param int compression_block_size: the compression block size in bytes.
    :param list bloom_filter_columns: list of columns to use Bloom filter.
    :param float bloom_filter_fpp: the false positive probability for the
        Bloom filter (Must be 0> and 1<).
    :param ZoneInfo timezone: a ZoneInfo object to use for writing timestamp
        columns.
    :param StructRepr struct_repr: An enum to set the representation for
        an ORC struct type.
    :param dict converters: a dictionary, where the keys are
        :class:`TypeKind` and the values are subclasses of
        :class:`ORCConverter`.
    :param float padding_tolerance: tolerance for block padding.
    :param float dict_key_size_threshold: threshold for dictionary encoding.
    :param object null_value: a singleton object to represent ORC null value.

.. method:: Writer.__enter__()
.. method:: Writer.__exit__()

    A context manager that automatically calls the :meth:`Writer.close` at
    the end of the ``with`` block.

.. method:: Writer.close()

    Close an ORC file and write out the metadata after the rows have been
    added. Must be called to get a valid ORC file.

.. method:: Writer.set_user_metadata(**kwargs)

    Set additional user metadata to the ORC file. The values must be bytes.
    The metadata is set when the Writer is closed.

    >>> out = open("test_metadata.orc", "wb")
    >>> wri = pyorc.Writer(out, "int")
    >>> wri.set_user_metadata(extra="info".encode())
    >>> wri.close()
    >>> inp = open("test_metadata.orc", "rb")
    >>> rdr = pyorc.Reader(inp)
    >>> rdr.user_metadata
    {'extra': b'info'}

    :param \*\*kwargs: keyword arguments to add as metadata to the file.

.. method:: Writer.write(row)

    Write a row to the ORC file.

    :param row: the row object to write.

.. method:: Writer.writerows(rows)

    Write multiple rows with one function call. It iterates over the `rows` and 
    calls :meth:`Writer.write`. Returns the written number of rows.

    :param iterable rows: an iterable with the rows.
    :return: the written number of rows.
    :rtype: int

.. attribute:: Writer.current_row

    The current row position.

.. attribute:: Writer.schema

    A read-only :class:`TypeDescription` object of the ORC file's schema.

Enums
=====

:class:`CompressionKind`
------------------------

.. autoclass:: pyorc.CompressionKind
    :members:
    :undoc-members:
    :member-order: bysource

:class:`CompressionStrategy`
----------------------------

.. autoclass:: pyorc.CompressionStrategy
    :members:
    :undoc-members:
    :member-order: bysource

:class:`TypeKind`
-----------------

.. autoclass:: pyorc.TypeKind
    :members:
    :undoc-members:
    :member-order: bysource

:class:`StructRepr`
-------------------

.. autoclass:: pyorc.StructRepr
    :members:
    :member-order: bysource

:class:`WriterVersion`
----------------------------

.. autoclass:: pyorc.WriterVersion
    :members:
    :undoc-members:
    :member-order: bysource
