#include "Converter.h"
#include "Reader.h"

#include "Writer.h"


namespace py = pybind11;

PYBIND11_MODULE(_pyorc, m) {
    m.doc() = "_pyorc c++ extension";
    py::class_<Stripe>(m, "stripe")
        .def(py::init([](Reader &reader, uint64_t num) {
                return reader.read_stripe(num);
            }))
        .def("__next__", [](Stripe &s) -> py::object { return s.next(); })
        .def("__iter__", [](Stripe &s) -> Stripe& { return s; })
        .def("__len__", &Stripe::len)
        .def("read", &Stripe::read)
        .def_property_readonly("bytes_length", [](Stripe &s) { return s.length(); })
        .def_property_readonly("bytes_offset", [](Stripe &s) { return s.offset(); })
        .def_property_readonly("writer_timezone", [](Stripe &s) { return s.writer_timezone(); })
        .def_readonly("current_row", &Stripe::currentRow);
    py::class_<Reader>(m, "reader")
        .def(py::init<py::object, py::object, py::object, py::object>(),
            py::arg("fileo"),
            py::arg_v("batch_size", py::int_(1024), "1024"),
            py::arg_v("col_indices", py::none(), "None"),
            py::arg_v("col_names", py::none(), "None"))
        .def("__next__", [](Reader &r) -> py::object { return r.next(); })
        .def("__iter__", [](Reader &r) -> Reader& { return r; })
        .def("__len__", &Reader::len)
        .def("read", &Reader::read)
        .def("read_stripe", &Reader::read_stripe)
        .def("seek", &Reader::seek)
        .def("schema", &Reader::schema)
        .def_property_readonly("num_of_stripes", [](Reader &r) { return r.numberOfStripes(); })
        .def_readonly("current_row", &Reader::currentRow);
    py::class_<Writer>(m, "writer")
        .def(py::init<py::object, py::object, py::object, py::object>(),
            py::arg("fileo"),
            py::arg("str_schema"),
            py::arg_v("batch_size", py::int_(1024), "1024"),
            py::arg_v("stripe_size", py::int_(67108864), "67108864"))
        .def("write", &Writer::write)
        .def("close", &Writer::close)
        .def_readonly("current_row", &Writer::currentRow);
}