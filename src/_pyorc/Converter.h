#ifndef CONVERTER_H
#define CONVERTER_H

#include <memory>

#include "orc/OrcFile.hh"

#include <pybind11/pybind11.h>

class Converter
{
  protected:
    bool hasNulls;
    const char* notNull = nullptr;

  public:
    Converter() = default;
    virtual ~Converter() = default;
    virtual pybind11::object toPython(uint64_t) = 0;
    virtual void write(orc::ColumnVectorBatch*, uint64_t, pybind11::object) = 0;
    virtual void reset(const orc::ColumnVectorBatch&);
    virtual void clear(){};
};

std::unique_ptr<Converter>
createConverter(const orc::Type*, unsigned int, pybind11::dict);

#endif