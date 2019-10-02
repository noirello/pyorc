#ifndef READER_H
#define READER_H

#include <pybind11/pybind11.h>
#include "orc/OrcFile.hh"

#include "Converter.h"

namespace py = pybind11;

class Reader {
private:
    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<orc::RowReader> rowReader;
    orc::RowReaderOptions rowReaderOpts;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<Converter> converter;
    uint64_t batchItem;
public:
    uint64_t currentRow;

    Reader(py::object, py::object = py::int_(1024), py::object = py::none(), py::object = py::none());
    uint64_t len();
    uint64_t numberOfStripes();
    uint64_t seek(uint64_t);
    py::object read();
    py::object next();
    py::object schema();
};

#endif