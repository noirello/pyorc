#include "Converter.h"
#include "Reader.h"

#include "Writer.h"

namespace py = pybind11;

PYBIND11_MODULE(_pyorc, m)
{
    m.doc() = "_pyorc c++ extension";
    py::class_<Stripe>(m, "stripe")
      .def(
        py::init([](Reader& reader, uint64_t num) { return reader.readStripe(num); }))
      .def("__next__", [](Stripe& s) -> py::object { return s.next(); })
      .def("__iter__", [](Stripe& s) -> Stripe& { return s; })
      .def("__len__", &Stripe::len)
      .def("read", &Stripe::read)
      .def("seek", &Stripe::seek, py::arg("row"), py::arg_v("whence", 0, "0"))
      .def_property_readonly("bytes_length", [](Stripe& s) { return s.length(); })
      .def_property_readonly("bytes_offset", [](Stripe& s) { return s.offset(); })
      .def_property_readonly("bloom_filter_columns",
                             [](Stripe& s) { return s.bloomFilterColumns(); })
      .def_property_readonly("writer_timezone",
                             [](Stripe& s) { return s.writerTimezone(); })
      .def_readonly("current_row", &Stripe::currentRow)
      .def_readonly("row_offset", &Stripe::firstRowOfStripe);
    py::class_<Reader>(m, "reader")
      .def(
        py::init<py::object, uint64_t, std::list<uint64_t>, std::list<std::string>>(),
        py::arg("fileo"),
        py::arg_v("batch_size", 1024, "1024"),
        py::arg_v("col_indices", std::list<uint64_t>{}, "None"),
        py::arg_v("col_names", std::list<std::string>{}, "None"))
      .def("__next__", [](Reader& r) -> py::object { return r.next(); })
      .def("__iter__", [](Reader& r) -> Reader& { return r; })
      .def("__len__", &Reader::len)
      .def("read", &Reader::read)
      .def("read_stripe", &Reader::readStripe, py::keep_alive<0, 1>())
      .def("seek", &Reader::seek, py::arg("row"), py::arg_v("whence", 0, "0"))
      .def("schema", &Reader::schema)
      .def_property_readonly("num_of_stripes",
                             [](Reader& r) { return r.numberOfStripes(); })
      .def_readonly("current_row", &Reader::currentRow);
    py::class_<Writer>(m, "writer")
      .def(py::init<py::object,
                    std::string,
                    uint64_t,
                    uint64_t,
                    int,
                    int,
                    std::set<uint64_t>>(),
           py::arg("fileo"),
           py::arg("str_schema"),
           py::arg_v("batch_size", 1024, "1024"),
           py::arg_v("stripe_size", 67108864, "67108864"),
           py::arg_v("compression", 1, "CompressionKind.ZLIB"),
           py::arg_v("compression_strategy", 0, "CompressionStrategy.SPEED"),
           py::arg_v("bloom_filter_cols", std::set<uint64_t>{}, "None"))
      .def("write", &Writer::write)
      .def("close", &Writer::close)
      .def_readonly("current_row", &Writer::currentRow);
}