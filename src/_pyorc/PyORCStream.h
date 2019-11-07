#ifndef PY_ORC_STREAM_H
#define PY_ORC_STREAM_H

#include <pybind11/pybind11.h>

#include "orc/OrcFile.hh"

namespace py = pybind11;

class PyORCInputStream : public orc::InputStream
{
  private:
    std::string filename;
    py::object pyread;
    py::object pyseek;
    uint64_t totalLength;

  public:
    PyORCInputStream(py::object);
    ~PyORCInputStream() override;
    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void*, uint64_t, uint64_t) override;
    const std::string& getName() const override;
};

class PyORCOutputStream : public orc::OutputStream
{
  private:
    std::string filename;
    py::object pywrite;
    py::object pyflush;
    uint64_t bytesWritten;
    bool closed;

  public:
    PyORCOutputStream(py::object);
    ~PyORCOutputStream() override;
    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    const std::string& getName() const override;
    void write(const void*, size_t) override;
    void close() override;
};

#endif
