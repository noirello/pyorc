#include "TypeDescription.h"

#include <iostream>
#include <sstream>

TypeDescription::TypeDescription(std::string schema)
{
    kindEnum = py::module::import("pyorc.enums").attr("TypeKind");
    try {
        auto orcType = orc::Type::buildTypeFromString(schema);
        setType(*orcType);
    } catch (std::logic_error err) {
        throw py::value_error(err.what());
    }
}

TypeDescription::TypeDescription(const orc::Type& orcType)
{
    kindEnum = py::module::import("pyorc.enums").attr("TypeKind");
    setType(orcType);
}

TypeDescription::TypeDescription(int kind_)
{
    kindEnum = py::module::import("pyorc.enums").attr("TypeKind");
    kind = kind_;
    columnId = 0;
    precision = py::none();
    scale = py::none();
    maxLength = py::none();
}

void
TypeDescription::setType(const orc::Type& orcType)
{
    fieldNames.clear();
    kind = static_cast<int>(orcType.getKind());
    columnId = orcType.getColumnId();
    precision = (kind == orc::DECIMAL) ? py::cast(orcType.getPrecision()) : py::none();
    scale = (kind == orc::DECIMAL) ? py::cast(orcType.getScale()) : py::none();
    if (kind == orc::CHAR || kind == orc::VARCHAR)
        maxLength = py::cast(orcType.getMaximumLength());
    else {
        maxLength = py::none();
    }
    containerTypes = py::list();
    if (kind == orc::LIST || kind == orc::MAP || kind == orc::UNION) {
        for (size_t i = 0; i < orcType.getSubtypeCount(); ++i) {
            auto type = py::cast(TypeDescription(*orcType.getSubtype(i)));
            containerTypes.append(type);
        }
    }
    fields = py::dict();
    if (kind == orc::STRUCT) {
        for (size_t i = 0; i < orcType.getSubtypeCount(); ++i) {
            auto key = orcType.getFieldName(i);
            fieldNames.push_back(key);
            auto field = py::cast(TypeDescription(*orcType.getSubtype(i)));
            fields[key.c_str()] = field;
        }
    }
}

std::unique_ptr<orc::Type>
TypeDescription::buildType()
{
    switch (kind) {
        case orc::BOOLEAN:
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
        case orc::FLOAT:
        case orc::DOUBLE:
        case orc::STRING:
        case orc::BINARY:
        case orc::TIMESTAMP:
        case orc::DATE:
            return orc::createPrimitiveType(static_cast<orc::TypeKind>(kind));
        case orc::VARCHAR:
        case orc::CHAR: {
            uint64_t length = 0;
            if (!maxLength.is(py::none())) {
                length = py::cast<uint64_t>(maxLength);
            }
            return orc::createCharType(static_cast<orc::TypeKind>(kind), length);
        }
        case orc::DECIMAL: {
            uint64_t prec_ = orc::DEFAULT_DECIMAL_PRECISION;
            uint64_t scale_ = orc::DEFAULT_DECIMAL_SCALE;
            if (!precision.is(py::none())) {
                prec_ = py::cast<uint64_t>(precision);
            }
            if (!scale.is(py::none())) {
                scale_ = py::cast<uint64_t>(scale);
            }
            return orc::createDecimalType(prec_, scale_);
        }
        case orc::LIST: {
            try {
                auto elem = py::cast<TypeDescription>(containerTypes[0]);
                return orc::createListType(elem.buildType());
            } catch (py::cast_error) {
                throw py::value_error(
                  "Items in container types must be a TypeDescription object.");
            }
        }
        case orc::MAP: {
            try {
                auto key = py::cast<TypeDescription>(containerTypes[0]);
                auto val = py::cast<TypeDescription>(containerTypes[1]);
                return orc::createMapType(key.buildType(), val.buildType());
            } catch (py::cast_error) {
                throw py::value_error(
                  "Items in container_types must be a TypeDescription object.");
            }
        }
        case orc::STRUCT: {
            auto type = orc::createStructType();
            for (auto name : fieldNames) {
                try {
                    auto field = py::cast<TypeDescription>(fields[name.c_str()]);
                    type->addStructField(name, field.buildType());
                } catch (py::cast_error) {
                    std::stringstream errmsg;
                    errmsg
                      << "Field `" << name
                      << "` has an invalid value. It must be a TypeDescription object.";
                    throw py::value_error(errmsg.str());
                }
            }
            return type;
        }
        case orc::UNION: {
            auto type = orc::createUnionType();
            for (size_t i = 0; i < containerTypes.size(); ++i) {
                try {
                    auto field = py::cast<TypeDescription>(containerTypes[i]);
                    type->addUnionChild(field.buildType());
                } catch (py::cast_error) {
                    std::stringstream errmsg;
                    errmsg << "Item " << i
                           << " in container_types has an invalid value. It must be a "
                              "TypeDescription object.";
                    throw py::value_error(errmsg.str());
                }
            }
            return type;
        }
        default:
            throw py::value_error("Invalid TypeKind");
    }
}

void
TypeDescription::addField(std::string name, TypeDescription type)
{
    if (kind != orc::STRUCT) {
        throw py::value_error("Not allowed to add field to a non struct type.");
    }
    if (!fields.contains(name)) {
        fieldNames.push_back(name);
    }
    fields[name.c_str()] = py::cast(type);
    setType(*buildType());
}

void
TypeDescription::removeField(std::string name)
{
    if (kind != orc::STRUCT) {
        throw py::value_error("Not allowed to remove field from a non struct type.");
    }
    fieldNames.erase(std::remove(fieldNames.begin(), fieldNames.end(), name),
                     fieldNames.end());
    int rc = PyDict_DelItemString(fields.ptr(), name.c_str());
    if (rc != 0) {
        throw py::error_already_set();
    }
    setType(*buildType());
}

uint64_t
TypeDescription::getColumnId()
{
    return columnId;
}

void
TypeDescription::setColumnId(uint64_t col)
{
    columnId = col;
}

py::object
TypeDescription::getContainerTypes()
{
    return containerTypes;
}

void
TypeDescription::setContainerTypes(py::object obj)
{
    py::list list(obj);
    if (kind != orc::LIST && kind != orc::MAP && kind != orc::UNION) {
        throw py::value_error("Not allowed to set container_type");
    }
    if (kind == orc::LIST && list.size() != 1) {
        throw py::value_error(
          "For list type container_types must contain one element.");
    }
    if (kind == orc::MAP && list.size() != 2) {
        throw py::value_error(
          "For map type container_types must contain two elements.");
    }
    if (kind == orc::UNION && list.size() == 0) {
        throw py::value_error("For union type container_types cannot be empty.");
    }
    for (size_t i = 0; i < list.size(); ++i) {
        if (!py::isinstance<TypeDescription>(list[i])) {
            std::stringstream errmsg;
            errmsg << "Item " << i
                   << " in container_types has an invalid value. It must be a "
                      "TypeDescription object.";
            throw py::value_error(errmsg.str());
        }
    }
    containerTypes = obj;
    setType(*buildType());
}

py::object
TypeDescription::getPrecision()
{
    return precision;
}

void
TypeDescription::setPrecision(uint64_t value)
{
    if (kind == orc::DECIMAL) {
        precision = py::cast<uint64_t>(value);
    } else {
        throw py::value_error("Cannot set precision for a non Decimal type.");
    }
}

py::object
TypeDescription::getScale()
{
    return scale;
}

void
TypeDescription::setScale(uint64_t value)
{
    if (kind == orc::DECIMAL) {
        scale = py::cast<uint64_t>(value);
    } else {
        throw py::value_error("Cannot set scale for a non Decimal type.");
    }
}

py::object
TypeDescription::getMaxLength()
{
    return maxLength;
}

void
TypeDescription::setMaxLength(uint64_t value)
{
    if (kind == orc::CHAR || kind == orc::VARCHAR) {
        maxLength = py::cast<uint64_t>(value);
    } else {
        throw py::value_error("Cannot set max_length for a non char or varchar type.");
    }
}

py::object
TypeDescription::getKind()
{
    return kindEnum(kind);
}

std::string
TypeDescription::str()
{
    return buildType()->toString();
}

uint64_t
TypeDescription::findColumnId(std::string colname)
{
    std::istringstream col(colname);
    std::string part;
    TypeDescription* type = this;
    while (getline(col, part, '.')) {
        type = py::cast<TypeDescription*>(type->fields[part.c_str()]);
    }
    return type->columnId;
}
