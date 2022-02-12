#include "Reader.h"
#include "Writer.h"
#include <orc/orc-config.hh>

namespace py = pybind11;

PYBIND11_MODULE(_pyorc, m)
{
    m.doc() = "_pyorc c++ extension";
    m.def("_orc_version", []() -> py::object { return py::cast(ORC_VERSION); });
    m.def("_schema_from_string", [](std::string schema) {
        try {
            auto orcType = orc::Type::buildTypeFromString(schema);
            return createTypeDescription(*orcType);
        } catch (std::logic_error& err) {
            throw py::value_error(err.what());
        }
    });
    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) {
                std::rethrow_exception(p);
            }
        } catch (const orc::ParseError& e) {
            py::object err = py::module::import("pyorc.errors").attr("ParseError");
            PyErr_SetString(err.ptr(), e.what());
        }
    });
    py::class_<Stripe>(m, "stripe")
      .def(
        py::init([](Reader& reader, uint64_t num) { return reader.readStripe(num); }),
        py::keep_alive<0, 2>())
      .def("__next__", [](Stripe& s) -> py::object { return s.next(); })
      .def("__iter__", [](Stripe& s) -> Stripe& { return s; })
      .def("__len__", &Stripe::len)
      .def("read", &Stripe::read, py::arg_v("num", -1, "-1"))
      .def("seek", &Stripe::seek, py::arg("row"), py::arg_v("whence", 0, "0"))
      .def("_statistics", &Stripe::statistics)
      .def_property_readonly("bytes_length", [](Stripe& s) { return s.length(); })
      .def_property_readonly("bytes_offset", [](Stripe& s) { return s.offset(); })
      .def_property_readonly("bloom_filter_columns",
                             [](Stripe& s) { return s.bloomFilterColumns(); })
      .def_property_readonly("writer_timezone",
                             [](Stripe& s) { return s.writerTimezone(); })
      .def_readonly("current_row", &Stripe::currentRow)
      .def_readonly("row_offset", &Stripe::firstRowOfStripe);
    py::class_<Reader>(m, "reader")
      .def(py::init<py::object,
                    uint64_t,
                    std::list<uint64_t>,
                    std::list<std::string>,
                    py::object,
                    unsigned int,
                    py::object,
                    py::object,
                    py::object>(),
           py::arg("fileo"),
           py::arg_v("batch_size", 1024, "1024"),
           py::arg_v("col_indices", std::list<uint64_t>{}, "None"),
           py::arg_v("col_names", std::list<std::string>{}, "None"),
           py::arg_v("timezone", py::none(), "None"),
           py::arg_v("struct_repr", 0, "StructRepr.TUPLE"),
           py::arg_v("conv", py::none(), "None"),
           py::arg_v("predicate", py::none(), "None"),
           py::arg_v("null_value", py::none(), "None"))
      .def("__next__", [](Reader& r) -> py::object { return r.next(); })
      .def("__iter__", [](Reader& r) -> Reader& { return r; })
      .def("__len__", &Reader::len)
      .def("read", &Reader::read, py::arg_v("num", -1, "-1"))
      .def("seek", &Reader::seek, py::arg("row"), py::arg_v("whence", 0, "0"))
      .def("_statistics", &Reader::statistics)
      .def_property_readonly("bytes_lengths", &Reader::bytesLengths)
      .def_property_readonly("compression", &Reader::compression)
      .def_property_readonly("compression_block_size", &Reader::compressionBlockSize)
      .def_property_readonly("row_index_stride", &Reader::rowIndexStride)
      .def_property_readonly("format_version", &Reader::formatVersion)
      .def_property_readonly("user_metadata", &Reader::userMetadata)
      .def_property_readonly("schema", &Reader::schema)
      .def_property_readonly("selected_schema", &Reader::selectedSchema)
      .def_property_readonly("num_of_stripes",
                             [](Reader& r) { return r.numberOfStripes(); })
      .def_property_readonly("writer_id", &Reader::writerId)
      .def_property_readonly("writer_version", &Reader::writerVersion)
      .def_property_readonly("software_version", &Reader::softwareVersion)
      .def_readonly("current_row", &Reader::currentRow);
    py::class_<Writer>(m, "writer")
      .def(py::init<py::object,
                    py::object,
                    uint64_t,
                    uint64_t,
                    uint64_t,
                    int,
                    int,
                    uint64_t,
                    std::set<uint64_t>,
                    double,
                    py::object,
                    unsigned int,
                    py::object,
                    double,
                    double,
                    py::object>(),
           py::arg("fileo"),
           py::arg("schema"),
           py::arg_v("batch_size", 1024, "1024"),
           py::arg_v("stripe_size", 67108864, "67108864"),
           py::arg_v("row_index_stride", 10000, "10000"),
           py::arg_v("compression", 1, "CompressionKind.ZLIB"),
           py::arg_v("compression_strategy", 0, "CompressionStrategy.SPEED"),
           py::arg_v("compression_block_size", 65536, "65536"),
           py::arg_v("bloom_filter_columns", std::set<uint64_t>{}, "None"),
           py::arg_v("bloom_filter_fpp", 0.05, "0.05"),
           py::arg_v("timezone", py::none(), "None"),
           py::arg_v("struct_repr", 0, "StructRepr.TUPLE"),
           py::arg_v("conv", py::none(), "None"),
           py::arg_v("padding_tolerance", 0.0, "0.0"),
           py::arg_v("dict_key_size_threshold", 0.0, "0.0"),
           py::arg_v("null_value", py::none(), "None"))
      .def("_add_user_metadata", &Writer::addUserMetadata)
      .def("write", &Writer::write)
      .def("writerows", &Writer::writerows)
      .def("close", &Writer::close)
      .def_readonly("current_row", &Writer::currentRow);
}
