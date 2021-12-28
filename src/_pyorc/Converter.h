#ifndef CONVERTER_H
#define CONVERTER_H

#include <memory>

#include "orc/OrcFile.hh"

#include <pybind11/pybind11.h>

namespace py = pybind11;
class Converter
{
  protected:
    bool hasNulls;
    const char* notNull = nullptr;
    py::object nullValue = py::none();

  public:
    Converter(py::object nv)
      : nullValue(nv){};
    virtual ~Converter() = default;
    virtual py::object toPython(uint64_t) = 0;
    virtual void write(orc::ColumnVectorBatch*, uint64_t, py::object) = 0;
    virtual void reset(const orc::ColumnVectorBatch&);
    virtual void clear(){};
};

std::unique_ptr<Converter>
createConverter(const orc::Type*, unsigned int, py::dict, py::object, py::object);

#endif
