API documentation
*****************

.. automodule:: pyorc

:class:`Reader`
===============

.. class:: Reader(fileo, batch_size=1024, column_indices=None, \
                  column_names=None, struct_repr=StructRepr.TUPLE, \
                  converters=None)

    An object to read ORC files. The `fileo` must be a binary stream that
    support seeking. Either `column_indices` or `column_names` can be used
    to select specific columns from the ORC file.

    The object iterates over rows by calling :meth:`Reader.__next__`. By
    default the ORC struct type represented as a tuple, but it can be change
    by changing `struct_repr` to a valid :class:`StructRepr` value.

    For decimal, date and timestamp ORC types the default converters to
    Python objects can be change by setting a dictionary to the `converters`
    parameter. The dictionary's keys must be a :class:`TypeKind` and the
    values must implement the ORCConverter abstract class.

    :param object fileo: a readable binary file-like object.
    :param int batch_size: The size of a batch to read.
    :param list column_indices: a list of column indices to read.
    :param list column_names: a list of column names to read.
    :param StructRepr struct_repr: An enum to set the representation for
        an ORC struct type.
    :param dict converters: a dictionary, where the keys are
        :class:`TypeKind` and the values are subclasses of ORCConverter.

.. method:: Reader.__next__()

    Get the next row from the file.

.. method:: Reader.__len__()

    Get the number of rows in the file.

.. method:: Reader.iter_stripes()

    Get an iterator with the :class:`stripe` objects from the file.

    :return: an iterator of :class:`stripe` objects.
    :rtype: iterator

.. method:: Reader.read(rows=-1)

    Read the rows into memory. If `rows` is specified, at most number of
    rows will be read.

    :return: A list of rows.
    :rtype: list

.. method:: Reader.read_stripe(idx)

    Read a specific :class:`stripe` object at `idx` from the ORC file.

    :param int idx: the index of the stripe.

    :return: a :class:`stripe` object.
    :rtype: stripe

.. method:: Reader.seek(row, whence=0)

    Jump to a certain row position in the file. Values for `whence` are:
        * 0 – start of the file (the default); offset should be zero or positive.
        * 1 – current file position; offset may be negative.
        * 2 – end of the file; offset should be negative.

    :return: number of the absolute row position.
    :rtype: int

.. attribute:: Reader.current_row

    The current row position.

.. attribute:: Reader.num_of_stripes

    The number of stripes in the ORC file.

.. attribute:: Reader.schema

    A :class:`typedescription` object of the ORC file's schema.


:class:`stripe`
===============

.. class:: stripe(reader, idx)

    An object that represents a stripe in an ORC file. It's iterable just
    like :class:`Reader`, and inherits many of its methods, but the read
    rows are limited to the stripe.

    :param Reader reader: a reader object.
    :param int idx: the index of the stripes.

.. method:: stripe.__next__()

    Get the next row from the stripe.

.. method:: stripe.__len__()

    Get the number of rows in the stripe.

.. method:: stripe.seek(row, whence=0)

    Jump to a certain row position in the stripe. For possible `whence`
    values see :meth:`Reader.seek`.

    :return: number of the absolute row position in the stripe.
    :rtype: int

.. method:: stripe.read(rows=-1)

    Read the rows into memory. If `rows` is specified, at most number of
    rows will be read.

    :return: A list of rows.
    :rtype: list

.. attribute:: stripe.bloom_filter_columns

    The list of column indices that have Bloom filter.

.. attribute:: stripe.bytes_length

    The length of the stripe in bytes.

.. attribute:: stripe.bytes_offset

    The bytes offset where the stripes starts in the file.

.. attribute:: stripe.current_row

    The current row position in the stripe.

.. attribute:: stripe.row_offset

    The row offset where the stripes starts in the file.

.. attribute:: stripe.writer_timezone

    The timezone information of the writer.

:class:`typedescription`
========================

.. class:: typedescription(str_schema)

    It represents an ORC schema, the hierarchy of the types in the file.

.. method:: typedescription.__str__()

    Get the string representation of the schema.

.. method:: typedescription.add_field(name, type)

    Add a new field to the type, when the current type is a struct.

.. method:: typedescription.find_column_id(name)

    Find the its id of a column by its name.

.. method:: typedescription.remove_field(name)

    Remove an existing field from the type, when the current type is a
    struct.

.. attribute:: typedescription.column_id

    The id of the column.

.. attribute:: typedescription.container_types

    If the current type is a container type, it returns a list of subtypes
    in the container as typedescription objects. For List it's a single
    list, for Map a pair. Uniontype can have multiple items.

.. attribute:: typedescription.fields

    A read-only dictionary of teh struct's fields, where the keys are the
    fields names and teh values are typedescription objects.

.. attribute:: typedescription.kind

    The kind of the current typedescription. It has to be one of the
    :class:`pyorc.TypeKind` enum values.

.. attribute:: typedescription.max_length

    The maximal length for a varchar type. If the typedescription is not a
    varchar then it's None.

.. attribute:: typedescription.precision

    The precision of a decimal type. If the typedescription is not a
    decimal then it's None.

.. attribute:: typedescription.scale

    The scale of a decimal type. If the typedescription is not a
    decimal then it's None.

:class:`Writer`
===============

.. class:: Writer(fileo, schema, batch_size=1024, \
                  stripe_size=67108864, compression=CompressionKind.ZLIB, \
                  compression_strategy=CompressionStrategy.SPEED, \
                  compression_block_size=65536, bloom_filter_columns=None, \
                  bloom_filter_fpp=0.05, struct_repr=StructRepr.TUPLE, \
                  converters=None)

    An object to write ORC files. The `fileo` must be a binary stream.
    The `schema` must be :class:`typedescription` or a valid ORC schema
    definition as a string.

    With the `bloom_filter_columns` a list of column ids or field names
    can be set to create a Bloom filter for the column. Nested structure
    fields can be selected with dotted format. For example a file with a
    ``struct<first:struct<second:int>>`` schema the second column can be
    selected as ``["first.second"]``.

    For decimal, date and timestamp ORC types the default converters from
    Python objects can be change by setting a dictionary to the `converters`
    parameter. The dictionary's keys must be a :class:`TypeKind` and the
    values must implement the ORCConverter abstract class.

    :param object fileo: a writeable binary file-like object.
    :param typedescription|str schema: the ORC schema of the file.
    :param int batch_size: the batch size for the ORC file.
    :param int stripe_size: the stripes size in bytes.
    :param CompressionKind compression: the compression kind for the ORC
        file.
    :param CompressionStrategy compression_strategy: the compression
        strategy.
    :param int compression_block_size: the compression block size in bytes.
    :param list bloom_filter_columns: list of columns to use Bloom filter.
    :param float bloom_filter_fpp: the false positive probability for the
        Bloom filter (Must be 0> and 1<).
    :param StructRepr struct_repr: An enum to set the representation for
        an ORC struct type.
    :param dict converters: a dictionary, where the keys are
        :class:`TypeKind` and the values are subclasses of ORCConverter.

.. method:: Writer. __enter__()
.. method:: Writer.__exit__()

    A context manager that automatically calls the :meth:`Writer.close` at
    the end of the ``with`` block.

.. method:: Writer.close()

    Close an ORC file and write out the metadata after the rows have been added.
    Must be called to get a valid ORC file.

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

    A :class:`typedescription` object of the ORC file's schema.

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
