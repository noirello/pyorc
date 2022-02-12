#ifndef WRITER_H
#define WRITER_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "orc/OrcFile.hh"

#include "Converter.h"

namespace py = pybind11;

class Writer
{
  private:
    std::unique_ptr<orc::OutputStream> outStream;
    std::unique_ptr<orc::Writer> writer;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<Converter> converter;
    uint64_t batchSize;
    uint64_t batchItem;

  public:
    uint64_t currentRow;

    Writer(py::object,
           py::object,
           uint64_t = 1024,
           uint64_t = 67108864,
           uint64_t = 10000,
           int = 1,
           int = 0,
           uint64_t = 65536,
           std::set<uint64_t> = {},
           double = 0.05,
           py::object = py::none(),
           unsigned int = 0,
           py::object = py::none(),
           double = 0.0,
           double = 0.0,
           py::object = py::none());
    void addUserMetadata(py::str, py::bytes);
    void write(py::object);
    uint64_t writerows(py::iterable);
    void close();
    ~Writer(){};
};

#endif
