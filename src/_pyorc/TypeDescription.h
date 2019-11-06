#ifndef TYPEDESCRIPTION_H
#define TYPEDESCRIPTION_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "orc/OrcFile.hh"

namespace py = pybind11;

class TypeDescription
{
  private:
    int kind;
    uint64_t columnId;
    std::vector<std::string> fieldNames;
    py::list containerTypes;
    py::object kindEnum;
    py::object precision;
    py::object scale;
    py::object maxLength;
    void setType(const orc::Type& orcType);

  public:

    py::dict fields;

    TypeDescription(const orc::Type&);
    TypeDescription(std::string);
    TypeDescription(int);
    std::string str();
    std::unique_ptr<orc::Type> buildType();
    void addField(std::string, TypeDescription);
    void removeField(std::string);
    uint64_t getColumnId();
    py::object getContainerTypes();
    void setContainerTypes(py::object);
    py::object getPrecision();
    void setPrecision(uint64_t);
    py::object getScale();
    void setScale(uint64_t);
    py::object getMaxLength();
    void setMaxLength(uint64_t);
    py::object getKind();
    uint64_t findColumnId(std::string);
};

#endif