#ifndef SEARCHARGUMENT_H
#define SEARCHARGUMENT_H

#include <orc/sargs/SearchArgument.hh>
#include <pybind11/pybind11.h>

namespace py = pybind11;

std::unique_ptr<orc::SearchArgument> createSearchArgument(py::object,
                                                          py::dict,
                                                          py::object);

#endif
