#include "Converter.h"
#include "Reader.h"

#include "Writer.h"


namespace py = pybind11;

PYBIND11_MODULE(_pyorc, m) {
    m.doc() = "_pyorc plugin";
    py::enum_<orc::TypeKind>(m, "TypeKind")
        .value("BOOLEAN", orc::TypeKind::BOOLEAN)
        .value("BYTE", orc::TypeKind::BYTE)
        .value("SHORT", orc::TypeKind::SHORT)
        .value("INT", orc::TypeKind::INT)
        .value("LONG", orc::TypeKind::LONG)
        .value("FLOAT", orc::TypeKind::FLOAT)
        .value("STRING", orc::TypeKind::STRING)
        .value("BINARY", orc::TypeKind::BINARY)
        .value("TIMESTAMP", orc::TypeKind::TIMESTAMP)
        .value("LIST", orc::TypeKind::LIST)
        .value("MAP", orc::TypeKind::MAP)
        .value("STRUCT", orc::TypeKind::STRUCT)
        .value("UNION", orc::TypeKind::UNION)
        .value("DECIMAL", orc::TypeKind::DECIMAL)
        .value("DATE", orc::TypeKind::DATE)
        .value("VARCHAR", orc::TypeKind::VARCHAR)
        .value("CHAR", orc::TypeKind::CHAR)
        .export_values();
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
        .def("seek", &Reader::seek)
        .def("schema", &Reader::schema)
        .def_property_readonly("num_of_stripes", [](Reader &r) { return r.numberOfStripes(); })
        .def_readonly("current_row", &Reader::currentRow);
    py::class_<Writer>(m, "writer")
        .def(py::init<py::object, py::object, py::object>(),
            py::arg("fileo"),
            py::arg("str_schema"),
            py::arg_v("batch_size", py::int_(1024), "1024"))
        .def("write", &Writer::write)
        .def("close", &Writer::close)
        .def_readonly("current_row", &Writer::currentRow);
}