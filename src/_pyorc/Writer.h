#ifndef WRITER_H
#define WRITER_H

#include <pybind11/pybind11.h>
#include "orc/OrcFile.hh"

#include "Converter.h"

namespace py = pybind11;

class Writer {
private:
    std::unique_ptr<orc::OutputStream> outStream;
    std::unique_ptr<orc::Writer> writer;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<Converter> converter;
    uint64_t batchSize;
    uint64_t batchItem;
public:
    uint64_t currentRow;

    Writer(py::object, py::object, py::object = py::int_(1024), py::object = py::int_(67108864));
    void write(py::object);
    void close();
};

#endif