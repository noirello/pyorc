Tutorial
========

At this point you have an installed pyorc module.

Reading
-------

Let's use one of the example ORC files to open in Python::

    >>> import pyorc
    >>> example = open("./deps/examples/demo-12-zlib.orc", "rb")
    >>> reader = pyorc.Reader(example)

See the schema of the selected file::

    >>> reader.schema
    <pyorc.typedescription.Struct object at 0x7f9784d91298>

The Reader's schema read-only property is a :class:`TypeDescription` object,
representing the ORC file's type hierarchy. We can get a more human-friendly
interpretation if we print its string format::

    >>> str(reader.schema)
    'struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>'

We can check the number of rows in the file by calling len() on the Reader::

    >>> len(reader)
    1920800

The Reader is an interable object, yielding a new row after every
iteration::

    >>> next(reader)
    (1, 'M', 'M', 'Primary', 500, 'Good', 0, 0, 0)
    >>> next(reader)
    (2, 'F', 'M', 'Primary', 500, 'Good', 0, 0, 0)

Iterating over the file's content to process its rows is the preferable way,
but we can also read the entire file into the memory with the read method.
This method has an optional parameter to control the maximal number of rows
to read::

    >>> rows = reader.read(10000)
    >>> rows
    ... (10000, 'F', 'U', 'Advanced Degree', 1500, 'Unknown', 1, 0, 0), (10001, 'M', 'M', 'Unknown', 1500, 'Unknown', 1, 0, 0), (10002, 'F', 'M', 'Unknown', 1500, 'Unknown', 1, 0, 0)]
    >>> reader.read()  # This call froze the interpreter for several minutes!
    ... (1920799, 'M', 'U', 'Unknown', 10000, 'Unknown', 6, 6, 6), (1920800, 'F', 'U', 'Unknown', 10000, 'Unknown', 6, 6, 6)]

Using this optional parameter for larger ORC file is highly recommended!

After all the rows are read, the Reader object has no more rows to yield.
There's a seek method to jump a specific row in the file and continue the
read from that point::

    >>> reader.seek(1000)
    1000
    >>> next(reader)
    (1001, 'M', 'M', 'College', 7500, 'Good', 0, 0, 0)

By default all fields are loaded from an ORC file, but that can be changed
by passing either `column_indices` or `column_names` parameter to Reader::

    >>> reader = pyorc.Reader(example, column_names=("_col0", "_col5"))
    >>> next(reader)
    (1, 'Good')

We can also change the representation of a struct from tuple to dictionary::

    >>> from pyorc.enums import StructRepr
    >>> reader = pyorc.Reader(example, column_indices=(1, 5), struct_repr=StructRepr.DICT)
    >>> next(reader)
    {'_col1': 'M', '_col5': 'Good'}

Stripes
-------

ORC files are divided in to stripes. Stripes are independent of each other.
Let's open an other ORC files that has multiple stripes in it::

    >>> example = open("./deps/examples/TestOrcFile.testStripeLevelStats.orc", "rb")
    >>> reader = pyorc.Reader(example)
    >>> reader.num_of_stripes
    3

The `num_of_stripes` property of the Reader shows how many stripes are in
the file. We can read a certain stripes using the `read_stripe` method::

    >>> stripe2 = reader.read_stripe(2)
    >>> stripe2
    <pyorc._pyorc.stripe object at 0x7f9784e09ce0>

The stripe object also an iterable object and has the same methods for
reading and seeking rows, but only in the boundaries of the selected
stripe::

    >>> next(stripe2)
    (3, 'three')
    >>> len(stripe1)
    1000
    >>> len(reader)
    11000
    >>> stripe2.row_offset
    10000

The `row_offset` returns the absolute position of the first row in the
stripe.

Filtering row groups
--------------------

It is possible to skip certain records in an ORC file using simple filter
predicates (or search arguments). Setting a predicate expression to the
Reader can help to exclude row groups that don't satisfy the condition
during reading::

    >>> example = open("./deps/examples/TestStringDictionary.testRowIndex.orc", "rb")
    >>> reader = pyorc.Reader(example)
    >>> next(reader)
    ('row 000000',)
    >>> reader = pyorc.Reader(example, predicate=pyorc.predicates.PredicateColumn(pyorc.TypeKind.STRING, "str") > "row 004096")
    >>> next(reader)
    ('row 004096',)

The predicate can be used to select a single row group, but not an
individual record. The size of the row group is determined by the
`row_index_stride`, set during writing of the file. You can create more
complex predicate using logical expressions::

    >>> pred = (PredicateColumn(TypeKind.INT, "c0") > 300) & (PredicateColumn(TypeKind.STRING, "c1") == "A")

One of the comparands must always be a literal value (cannot compare two
columns to each other).

Writing
-------

To write a new ORC file we need to open a binary file-like object and pass
to a Writer object with an ORC schema description. The schema can be a 
TypeDescription or a simple string ORC schema definition::

    >>> output = open("./new.orc", "wb")
    >>> writer = pyorc.Writer(output, "struct<col0:int,col1:string>")
    >>> writer
    <pyorc.writer.Writer object at 0x7f9784e0c308>

We can add rows to the file with the `write` method::

    >>> writer.write((0, "Test 0"))
    >>> writer.write((1, "Test 1"))

Don't forget to close the writer to write out the necessary metadata,
otherwise it won't be a valid ORC file.

    >>> writer.close()

For simpler use the Writer object can be used as a context manager and you
can also change the struct representation to use dictionaries as rows instead
of tuples as well:

.. code-block:: python

    with open("./new.orc", "wb") as output:
        with pyorc.Writer(output, "struct<col0:int,col1:string>", struct_repr=StructRepr.DICT) as writer:
            writer.write({"col0": 0, "col1": "Test 0"})


Using custom converters
-----------------------

It's possible to change the default converters that handle the transformations
from ORC `date`, `decimal`, and `timestamp` types to Python objects, and back.
To create your own converter you need to implement the :class:`ORCConverter`
abstract class with two methods: ``from_orc`` and ``to_orc``. The following
example returns the ORC timestamp values as seconds and nanoseconds pair:

.. code-block:: python

    import pyorc
    from pyorc.converters import ORCConverter

    class TSConverter(ORCConverter):
        @staticmethod
        def to_orc(*args):
            seconds, nanoseconds, timezone = args
            return (seconds, nanoseconds)

        @staticmethod
        def from_orc(seconds, nanoseconds, timezone):
            return (seconds, nanoseconds)

To use the converter you have to set the Reader's or Writer's converters
parameter as a dictionary with one of the supported types as key::

    data = open("./timestamps.orc", "rb")
    reader = pyorc.Reader(data, converters={TypeKind.TIMESTAMP: TSConverter})
