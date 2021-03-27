#include <orc/Type.hh>

#include "SearchArgument.h"

std::tuple<orc::PredicateDataType, orc::Literal>
buildLiteral(py::object column, py::object value, py::dict convDict)
{
    int colType = py::cast<int>(column.attr("type_kind"));
    switch (colType) {
        case orc::TypeKind::BOOLEAN:
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::BOOLEAN,
                                       orc::Literal(orc::PredicateDataType::BOOLEAN));
            } else {
                return std::make_tuple(orc::PredicateDataType::BOOLEAN,
                                       orc::Literal(py::cast<bool>(value)));
            }
        case orc::TypeKind::BYTE:
        case orc::TypeKind::SHORT:
        case orc::TypeKind::INT:
        case orc::TypeKind::LONG:
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::LONG,
                                       orc::Literal(orc::PredicateDataType::LONG));
            } else {
                return std::make_tuple(orc::PredicateDataType::LONG,
                                       orc::Literal(py::cast<int64_t>(value)));
            }
        case orc::TypeKind::FLOAT:
        case orc::TypeKind::DOUBLE:
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::FLOAT,
                                       orc::Literal(orc::PredicateDataType::FLOAT));
            } else {
                return std::make_tuple(orc::PredicateDataType::FLOAT,
                                       orc::Literal(py::cast<double>(value)));
            }
        case orc::TypeKind::CHAR:
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::STRING: {
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::STRING,
                                       orc::Literal(orc::PredicateDataType::STRING));
            } else {
                std::string str = py::cast<std::string>(value);
                return std::make_tuple(orc::PredicateDataType::STRING,
                                       orc::Literal(str.c_str(), str.size()));
            }
        }
        case orc::TypeKind::DATE: {
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::DATE,
                                       orc::Literal(orc::PredicateDataType::DATE));
            } else {
                py::object idx(py::int_(static_cast<int>(orc::TypeKind::DATE)));
                py::object to_orc = convDict[idx].attr("to_orc");
                return std::make_tuple(orc::PredicateDataType::DATE,
                                       orc::Literal(orc::PredicateDataType::DATE,
                                                    py::cast<int64_t>(to_orc(value))));
            }
        }
        case orc::TypeKind::TIMESTAMP: {
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::TIMESTAMP,
                                       orc::Literal(orc::PredicateDataType::TIMESTAMP));
            } else {
                py::object idx(py::int_(static_cast<int>(orc::TypeKind::TIMESTAMP)));
                py::object to_orc = convDict[idx].attr("to_orc");
                py::tuple data = to_orc(value);
                py::tuple res = to_orc(value);
                return std::make_tuple(
                  orc::PredicateDataType::TIMESTAMP,
                  orc::Literal(py::cast<int64_t>(res[0]), py::cast<int64_t>(res[1])));
            }
        }
        case orc::TypeKind::DECIMAL: {
            if (value.is_none()) {
                return std::make_tuple(orc::PredicateDataType::DECIMAL,
                                       orc::Literal(orc::PredicateDataType::DECIMAL));
            } else {
                py::object idx(py::int_(static_cast<int>(orc::TypeKind::DECIMAL)));
                uint64_t precision = py::cast<uint64_t>(column.attr("precision"));
                uint64_t scale = py::cast<uint64_t>(column.attr("scale"));
                py::object to_orc = convDict[idx].attr("to_orc");
                py::object value = to_orc(precision, scale, value);
                std::string strVal = py::cast<std::string>(py::str(value));
                return std::make_tuple(orc::PredicateDataType::DECIMAL,
                                       orc::Literal(orc::Int128(strVal),
                                                    static_cast<int32_t>(precision),
                                                    static_cast<int32_t>(scale)));
            }
        }
        default:
            throw py::type_error("Unsupported type for ORC Literal in predicate");
    }
}

orc::SearchArgumentBuilder&
buildSearchArgument(orc::SearchArgumentBuilder& sarg,
                    py::tuple predVals,
                    py::dict convDict)
{
    int opCode = py::cast<int>(predVals[0]);
    switch (opCode) {
        case 0: /* NOT */
            return buildSearchArgument(sarg.startNot(), predVals[1], convDict).end();
        case 1: /* OR */
            return buildSearchArgument(
                     buildSearchArgument(sarg.startOr(), predVals[1], convDict),
                     predVals[2],
                     convDict)
              .end();
        case 2: /* AND */
            return buildSearchArgument(
                     buildSearchArgument(sarg.startAnd(), predVals[1], convDict),
                     predVals[2],
                     convDict)
              .end();
        case 3: { /* EQ */
            std::string colName = py::cast<std::string>(predVals[1].attr("name"));
            std::tuple<orc::PredicateDataType, orc::Literal> res =
              buildLiteral(predVals[1], predVals[2], convDict);
            return sarg.equals(colName, std::get<0>(res), std::get<1>(res));
        }
        case 4: { /* LT */
            std::string colName = py::cast<std::string>(predVals[1].attr("name"));
            std::tuple<orc::PredicateDataType, orc::Literal> res =
              buildLiteral(predVals[1], predVals[2], convDict);
            return sarg.lessThan(colName, std::get<0>(res), std::get<1>(res));
        }
        case 5: { /* LE */
            std::string colName = py::cast<std::string>(predVals[1].attr("name"));
            std::tuple<orc::PredicateDataType, orc::Literal> res =
              buildLiteral(predVals[1], predVals[2], convDict);
            return sarg.lessThanEquals(colName, std::get<0>(res), std::get<1>(res));
        }
        default:
            throw py::type_error("Invalid operation on Literal in predicate");
    }
    return sarg;
}

std::unique_ptr<orc::SearchArgument>
createSearchArgument(py::object predicate, py::dict convDict)
{
    std::unique_ptr<orc::SearchArgumentBuilder> builder =
      orc::SearchArgumentFactory::newBuilder();
    try {
        py::tuple predVals = predicate.attr("values");
        buildSearchArgument(*builder.get(), predVals, convDict);
        return builder->build();
    } catch (py::error_already_set& err) {
        if (err.matches(PyExc_AttributeError)) {
            std::string strbuf("Invalid predicate: ");
            strbuf.append(py::cast<std::string>(py::repr(predicate)).c_str());
            throw py::type_error(strbuf.c_str());
        } else {
            throw;
        }
    }
}
