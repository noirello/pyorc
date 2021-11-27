#include <sstream>

#include "Converter.h"

class BoolConverter : public Converter
{
  private:
    const int64_t* data;

  public:
    BoolConverter(py::object nv)
      : Converter(nv)
      , data(nullptr)
    {}
    ~BoolConverter() override {}
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class LongConverter : public Converter
{
  private:
    const int64_t* data;

  public:
    LongConverter(py::object nv)
      : Converter(nv)
      , data(nullptr)
    {}
    ~LongConverter() override {}
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class DoubleConverter : public Converter
{
  private:
    const double* data;

  public:
    DoubleConverter(py::object nv)
      : Converter(nv)
      , data(nullptr)
    {}
    ~DoubleConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class StringConverter : public Converter
{
  private:
    const char* const* data;
    const int64_t* length;
    std::vector<py::object> buffer;

  public:
    StringConverter(py::object nv)
      : Converter(nv)
      , data(nullptr)
      , length(nullptr)
    {}
    ~StringConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class BinaryConverter : public Converter
{
  private:
    const char* const* data;
    const int64_t* length;
    std::vector<py::object> buffer;

  public:
    BinaryConverter(py::object nv)
      : Converter(nv)
      , data(nullptr)
      , length(nullptr)
    {}
    ~BinaryConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class TimestampConverter : public Converter
{
  private:
    const int64_t* seconds;
    const int64_t* nanoseconds;
    py::object to_orc;
    py::object from_orc;
    py::object timeZoneInfo;

  public:
    TimestampConverter(py::dict conv, py::object tzone, py::object nv);
    virtual ~TimestampConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class DateConverter : public Converter
{
  private:
    const int64_t* data;
    py::object to_orc;
    py::object from_orc;

  public:
    DateConverter(py::dict conv, py::object nv);
    virtual ~DateConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class Decimal64Converter : public Converter
{
  private:
    const int64_t* data;
    uint64_t prec;
    int32_t scale;
    py::object to_orc;
    py::object from_orc;

  public:
    Decimal64Converter(uint64_t prec_, uint64_t scale_, py::dict conv, py::object nv);
    virtual ~Decimal64Converter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class Decimal128Converter : public Converter
{
  private:
    const orc::Int128* data;
    uint64_t prec;
    int32_t scale;
    py::object to_orc;
    py::object from_orc;

  public:
    Decimal128Converter(uint64_t prec_, uint64_t scale_, py::dict conv, py::object nv);
    virtual ~Decimal128Converter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
};

class ListConverter : public Converter
{
  private:
    const int64_t* offsets;
    std::unique_ptr<Converter> elementConverter;

  public:
    ListConverter(const orc::Type& type,
                  unsigned int structKind,
                  py::dict conv,
                  py::object tzone,
                  py::object nv);
    virtual ~ListConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class MapConverter : public Converter
{
  private:
    const int64_t* offsets;
    std::unique_ptr<Converter> keyConverter;
    std::unique_ptr<Converter> elementConverter;

  public:
    MapConverter(const orc::Type& type,
                 unsigned int structKind,
                 py::dict conv,
                 py::object tzone,
                 py::object nv);
    virtual ~MapConverter() override{};
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class UnionConverter : public Converter
{
  private:
    const unsigned char* tags;
    const uint64_t* offsets;
    std::vector<Converter*> fieldConverters;
    std::map<unsigned char, uint64_t> childOffsets;

  public:
    UnionConverter(const orc::Type& type,
                   unsigned int structKind,
                   py::dict conv,
                   py::object tzone,
                   py::object nv);
    virtual ~UnionConverter() override;
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class StructConverter : public Converter
{
  private:
    std::vector<Converter*> fieldConverters;
    std::vector<py::str> fieldNames;
    unsigned int kind;

  public:
    StructConverter(const orc::Type& type,
                    unsigned int kind_,
                    py::dict conv,
                    py::object tzone,
                    py::object nv);
    virtual ~StructConverter() override;
    py::object toPython(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

std::unique_ptr<Converter>
createConverter(const orc::Type* type,
                unsigned int structKind,
                py::dict conv,
                py::object tzone,
                py::object nullValue)
{
    Converter* result = nullptr;
    if (structKind > 1) {
        throw py::value_error("Invalid struct kind");
    }
    if (type == nullptr) {
        throw py::value_error("Received an invalid type");
    } else {
        switch (static_cast<int64_t>(type->getKind())) {
            case orc::BOOLEAN:
                result = new BoolConverter(nullValue);
                break;
            case orc::BYTE:
            case orc::SHORT:
            case orc::INT:
            case orc::LONG:
                result = new LongConverter(nullValue);
                break;
            case orc::FLOAT:
            case orc::DOUBLE:
                result = new DoubleConverter(nullValue);
                break;
            case orc::STRING:
            case orc::VARCHAR:
            case orc::CHAR:
                result = new StringConverter(nullValue);
                break;
            case orc::BINARY:
                result = new BinaryConverter(nullValue);
                break;
            case orc::TIMESTAMP:
            case orc::TIMESTAMP_INSTANT:
                result = new TimestampConverter(conv, tzone, nullValue);
                break;
            case orc::LIST:
                result = new ListConverter(*type, structKind, conv, tzone, nullValue);
                break;
            case orc::MAP:
                result = new MapConverter(*type, structKind, conv, tzone, nullValue);
                break;
            case orc::STRUCT:
                result = new StructConverter(*type, structKind, conv, tzone, nullValue);
                break;
            case orc::DECIMAL:
                if (type->getPrecision() == 0 || type->getPrecision() > 18) {
                    result = new Decimal128Converter(
                      type->getPrecision(), type->getScale(), conv, nullValue);
                } else {
                    result = new Decimal64Converter(
                      type->getPrecision(), type->getScale(), conv, nullValue);
                }
                break;
            case orc::DATE:
                result = new DateConverter(conv, nullValue);
                break;
            case orc::UNION:
                result = new UnionConverter(*type, structKind, conv, tzone, nullValue);
                break;
            default:
                throw py::value_error("unknown batch type");
        }
    }
    return std::unique_ptr<Converter>(result);
}

void
Converter::reset(const orc::ColumnVectorBatch& batch)
{
    hasNulls = batch.hasNulls;
    if (hasNulls) {
        notNull = batch.notNull.data();
    } else {
        notNull = nullptr;
    }
}

void
BoolConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}

py::object
BoolConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return py::bool_(data[rowId]);
    }
}

void
BoolConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        longBatch->hasNulls = true;
        longBatch->notNull[rowId] = 0;
    } else {
        try {
            longBatch->data[rowId] = py::cast<int64_t>(elem);
            longBatch->notNull[rowId] = 1;
        } catch (py::cast_error&) {
            std::stringstream errmsg;
            errmsg << "Item " << (std::string)(std::string)py::repr(elem)
                   << " cannot be cast to long int (for boolean)";
            throw py::type_error(errmsg.str());
        }
    }
    longBatch->numElements = rowId + 1;
}

void
LongConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}

py::object
LongConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return py::cast(data[rowId]);
    }
}

void
LongConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        longBatch->hasNulls = true;
        longBatch->notNull[rowId] = 0;
    } else {
        try {
            longBatch->data[rowId] = py::cast<int64_t>(elem);
            longBatch->notNull[rowId] = 1;
        } catch (py::cast_error&) {
            std::stringstream errmsg;
            errmsg << "Item " << (std::string)py::repr(elem)
                   << " cannot be cast to long int";
            throw py::type_error(errmsg.str());
        }
    }
    longBatch->numElements = rowId + 1;
}

void
DoubleConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    data = dynamic_cast<const orc::DoubleVectorBatch&>(batch).data.data();
}

py::object
DoubleConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return py::cast(data[rowId]);
    }
}

void
DoubleConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* doubleBatch = dynamic_cast<orc::DoubleVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        doubleBatch->hasNulls = true;
        doubleBatch->notNull[rowId] = 0;
    } else {
        try {
            doubleBatch->data[rowId] = py::cast<double>(elem);
            doubleBatch->notNull[rowId] = 1;
        } catch (py::cast_error&) {
            std::stringstream errmsg;
            errmsg << "Item " << (std::string)py::repr(elem)
                   << " cannot be cast to double";
            throw py::type_error(errmsg.str());
        }
    }
    doubleBatch->numElements = rowId + 1;
}

void
StringConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& strBatch = dynamic_cast<const orc::StringVectorBatch&>(batch);
    data = strBatch.data.data();
    length = strBatch.length.data();
}

py::object
StringConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return py::str(data[rowId], static_cast<size_t>(length[rowId]));
    }
}

void
StringConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* strBatch = dynamic_cast<orc::StringVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        strBatch->hasNulls = true;
        strBatch->notNull[rowId] = 0;
    } else {
        Py_ssize_t length = 0;
        const char* src = PyUnicode_AsUTF8AndSize(elem.ptr(), &length);
        if (src == nullptr) {
            if (PyErr_ExceptionMatches(PyExc_TypeError) == 1) {
                PyErr_Clear();
                std::stringstream errmsg;
                errmsg << "Item " << (std::string)py::repr(elem)
                       << " cannot be cast to string";
                throw py::type_error(errmsg.str());
            } else {
                throw py::error_already_set();
            }
        }
        buffer.push_back(elem);
        strBatch->data[rowId] = const_cast<char*>(src);
        strBatch->length[rowId] = static_cast<int64_t>(length);
        strBatch->notNull[rowId] = 1;
    }
    strBatch->numElements = rowId + 1;
}

void
StringConverter::clear()
{
    buffer.clear();
}

void
BinaryConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& strBatch = dynamic_cast<const orc::StringVectorBatch&>(batch);
    data = strBatch.data.data();
    length = strBatch.length.data();
}

py::object
BinaryConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return py::bytes(data[rowId], static_cast<size_t>(length[rowId]));
    }
}

void
BinaryConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    char* src = nullptr;
    auto* bytesBatch = dynamic_cast<orc::StringVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        bytesBatch->hasNulls = true;
        bytesBatch->notNull[rowId] = 0;
    } else {
        Py_ssize_t length = 0;
        int rc = PyBytes_AsStringAndSize(elem.ptr(), &src, &length);
        if (rc == -1) {
            if (PyErr_ExceptionMatches(PyExc_TypeError) == 1) {
                PyErr_Clear();
                std::stringstream errmsg;
                errmsg << "Item " << (std::string)py::repr(elem)
                       << " cannot be cast to bytes";
                throw py::type_error(errmsg.str());
            } else {
                throw py::error_already_set();
            }
        }
        buffer.push_back(elem);
        bytesBatch->data[rowId] = src;
        bytesBatch->length[rowId] = static_cast<int64_t>(length);
        bytesBatch->notNull[rowId] = 1;
    }
    bytesBatch->numElements = rowId + 1;
}

void
BinaryConverter::clear()
{
    buffer.clear();
}

TimestampConverter::TimestampConverter(py::dict conv, py::object tzone, py::object nv)
  : Converter(nv)
  , seconds(nullptr)
  , nanoseconds(nullptr)
{
    py::object idx(py::int_(static_cast<int>(orc::TIMESTAMP)));
    timeZoneInfo = tzone;
    from_orc = conv[idx].attr("from_orc");
    to_orc = conv[idx].attr("to_orc");
}

void
TimestampConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& ts = dynamic_cast<const orc::TimestampVectorBatch&>(batch);
    seconds = ts.data.data();
    nanoseconds = ts.nanoseconds.data();
}

py::object
TimestampConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return from_orc(seconds[rowId], nanoseconds[rowId], timeZoneInfo);
    }
}

void
TimestampConverter::write(orc::ColumnVectorBatch* batch,
                          uint64_t rowId,
                          py::object elem)
{
    auto* tsBatch = dynamic_cast<orc::TimestampVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        tsBatch->hasNulls = true;
        tsBatch->notNull[rowId] = 0;
    } else {
        try {
            py::tuple res = to_orc(elem, timeZoneInfo);
            tsBatch->data[rowId] = py::cast<int64_t>(res[0]);
            tsBatch->nanoseconds[rowId] = py::cast<int64_t>(res[1]);
            tsBatch->notNull[rowId] = 1;
        } catch (py::error_already_set& ex) {
            if (!ex.matches(PyExc_AttributeError)) {
                throw;
            } else {
                PyErr_Clear();
                std::stringstream errmsg;
                errmsg << "Item " << (std::string)py::repr(elem)
                       << " cannot be cast to timestamp";
                throw py::type_error(errmsg.str());
            }
        }
    }
    tsBatch->numElements = rowId + 1;
}

DateConverter::DateConverter(py::dict conv, py::object nv)
  : Converter(nv)
  , data(nullptr)
{
    py::object idx(py::int_(static_cast<int>(orc::DATE)));
    from_orc = conv[idx].attr("from_orc");
    to_orc = conv[idx].attr("to_orc");
}

void
DateConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}

py::object
DateConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return from_orc(data[rowId]);
    }
}

void
DateConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* dBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        dBatch->hasNulls = true;
        dBatch->notNull[rowId] = 0;
    } else {
        dBatch->data[rowId] = py::cast<int64_t>(to_orc(elem));
        dBatch->notNull[rowId] = 1;
    }
    dBatch->numElements = rowId + 1;
}

Decimal64Converter::Decimal64Converter(uint64_t prec_,
                                       uint64_t scale_,
                                       py::dict conv,
                                       py::object nv)
  : Converter(nv)
  , data(nullptr)
  , prec(prec_)
  , scale(scale_)
{
    py::object idx(py::int_(static_cast<int>(orc::DECIMAL)));
    from_orc = conv[idx].attr("from_orc");
    to_orc = conv[idx].attr("to_orc");
}

void
Decimal64Converter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& decBatch = dynamic_cast<const orc::Decimal64VectorBatch&>(batch);
    data = decBatch.values.data();
    scale = decBatch.scale;
}

std::string
toDecimalString(int64_t value, int32_t scale)
{
    std::stringstream buffer;
    if (scale == 0) {
        buffer << value;
        return buffer.str();
    }
    std::string sign = "";
    if (value < 0) {
        sign = "-";
        value = -value;
    }
    buffer << value;
    std::string str = buffer.str();
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
        return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." +
               str.substr(static_cast<size_t>(len - scale), static_cast<size_t>(scale));
    } else if (len == scale) {
        return sign + "0." + str;
    } else {
        std::string result = sign + "0.";
        for (int32_t i = 0; i < scale - len; ++i) {
            result += "0";
        }
        return result + str;
    }
}

py::object
Decimal64Converter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return from_orc(toDecimalString(data[rowId], scale));
    }
}

void
Decimal64Converter::write(orc::ColumnVectorBatch* batch,
                          uint64_t rowId,
                          py::object elem)
{
    auto* decBatch = dynamic_cast<orc::Decimal64VectorBatch*>(batch);
    decBatch->precision = prec;
    decBatch->scale = scale;
    if (elem.is(nullValue)) {
        decBatch->hasNulls = true;
        decBatch->notNull[rowId] = 0;
    } else {
        py::object value = to_orc(decBatch->precision, decBatch->scale, elem);
        try {
            decBatch->values[rowId] = py::cast<int64_t>(value);
            decBatch->notNull[rowId] = 1;
        } catch (py::cast_error&) {
            std::stringstream errmsg;
            errmsg << "Item " << (std::string)py::repr(elem)
                   << " cannot be cast to long int (for decimal)";
            throw py::type_error(errmsg.str());
        }
    }
    decBatch->numElements = rowId + 1;
}

Decimal128Converter::Decimal128Converter(uint64_t prec_,
                                         uint64_t scale_,
                                         py::dict conv,
                                         py::object nv)
  : Converter(nv)
  , data(nullptr)
  , prec(prec_)
  , scale(scale_)
{
    py::object idx(py::int_(static_cast<int>(orc::DECIMAL)));
    from_orc = conv[idx].attr("from_orc");
    to_orc = conv[idx].attr("to_orc");
}

void
Decimal128Converter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& decBatch = dynamic_cast<const orc::Decimal128VectorBatch&>(batch);
    data = decBatch.values.data();
    scale = decBatch.scale;
}

py::object
Decimal128Converter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return from_orc(data[rowId].toDecimalString(scale));
    }
}

void
Decimal128Converter::write(orc::ColumnVectorBatch* batch,
                           uint64_t rowId,
                           py::object elem)
{
    auto* decBatch = dynamic_cast<orc::Decimal128VectorBatch*>(batch);
    decBatch->precision = prec;
    decBatch->scale = scale;
    if (elem.is(nullValue)) {
        decBatch->hasNulls = true;
        decBatch->notNull[rowId] = 0;
    } else {
        py::object value = to_orc(decBatch->precision, decBatch->scale, elem);
        try {
            std::string strVal = py::cast<std::string>(py::str(value));
            decBatch->values[rowId] = orc::Int128(strVal);
            decBatch->notNull[rowId] = 1;
        } catch (py::cast_error&) {
            std::stringstream errmsg;
            errmsg << "Item " << (std::string)py::repr(elem)
                   << " cannot be cast to decimal128";
            throw py::type_error(errmsg.str());
        }
    }
    decBatch->numElements = rowId + 1;
}

ListConverter::ListConverter(const orc::Type& type,
                             unsigned int structKind,
                             py::dict conv,
                             py::object tzone,
                             py::object nv)
  : Converter(nv)
  , offsets(nullptr)
{
    elementConverter = createConverter(type.getSubtype(0), structKind, conv, tzone, nv);
}

void
ListConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& listBatch = dynamic_cast<const orc::ListVectorBatch&>(batch);
    offsets = listBatch.offsets.data();
    elementConverter->reset(*listBatch.elements);
}

py::object
ListConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        py::list result;
        for (int64_t i = offsets[rowId]; i < offsets[rowId + 1]; ++i) {
            result.append(elementConverter->toPython(static_cast<uint64_t>(i)));
        }
        return result;
    }
}

void
ListConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    size_t size = 0;
    auto* listBatch = dynamic_cast<orc::ListVectorBatch*>(batch);
    listBatch->offsets[0] = 0;
    uint64_t offset = static_cast<uint64_t>(listBatch->offsets[rowId]);
    if (elem.is(nullValue)) {
        listBatch->hasNulls = true;
        listBatch->notNull[rowId] = 0;
    } else {
        py::list list(elem);
        size = list.size();
        if (listBatch->elements->capacity < offset + size) {
            listBatch->elements->resize(2 * (offset + size));
        }
        for (size_t cnt = 0; cnt < size; ++cnt) {
            elementConverter->write(listBatch->elements.get(), offset + cnt, list[cnt]);
        }
        listBatch->notNull[rowId] = 1;
    }
    listBatch->offsets[rowId + 1] = offset + size;
    listBatch->numElements = rowId + 1;
}

void
ListConverter::clear()
{
    elementConverter->clear();
}

MapConverter::MapConverter(const orc::Type& type,
                           unsigned int structKind,
                           py::dict conv,
                           py::object tzone,
                           py::object nv)
  : Converter(nv)
  , offsets(nullptr)
{
    keyConverter = createConverter(type.getSubtype(0), structKind, conv, tzone, nv);
    elementConverter = createConverter(type.getSubtype(1), structKind, conv, tzone, nv);
}

void
MapConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& mapBatch = dynamic_cast<const orc::MapVectorBatch&>(batch);
    offsets = mapBatch.offsets.data();
    keyConverter->reset(*mapBatch.keys);
    elementConverter->reset(*mapBatch.elements);
}

py::object
MapConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        py::dict result;
        for (int64_t i = offsets[rowId]; i < offsets[rowId + 1]; ++i) {
            py::object key = keyConverter->toPython(static_cast<uint64_t>(i));
            result[key] = elementConverter->toPython(static_cast<uint64_t>(i));
        }
        return result;
    }
}

void
MapConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    uint64_t cnt = 0;
    auto* mapBatch = dynamic_cast<orc::MapVectorBatch*>(batch);
    mapBatch->offsets[0] = 0;
    uint64_t offset = static_cast<uint64_t>(mapBatch->offsets[rowId]);
    if (elem.is(nullValue)) {
        mapBatch->hasNulls = true;
        mapBatch->notNull[rowId] = 0;
    } else {
        py::dict dict(elem);
        size_t size = dict.size();
        if (mapBatch->keys->capacity < offset + size) {
            mapBatch->elements->resize(2 * (offset + size));
            mapBatch->keys->resize(2 * (offset + size));
        }
        for (auto item : dict) {
            py::object key = py::reinterpret_borrow<py::object>(item.first);
            py::object val = py::reinterpret_borrow<py::object>(item.second);
            keyConverter->write(mapBatch->keys.get(), cnt + offset, key);
            elementConverter->write(mapBatch->elements.get(), cnt + offset, val);
            ++cnt;
        }
        mapBatch->notNull[rowId] = 1;
    }
    mapBatch->offsets[rowId + 1] = offset + cnt;
    mapBatch->numElements = rowId + 1;
}

void
MapConverter::clear()
{
    keyConverter->clear();
    elementConverter->clear();
}

UnionConverter::UnionConverter(const orc::Type& type,
                               unsigned int structKind,
                               py::dict conv,
                               py::object tzone,
                               py::object nv)
  : Converter(nv)
  , tags(nullptr)
  , offsets(nullptr)
{
    for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
        fieldConverters.push_back(
          createConverter(type.getSubtype(i), structKind, conv, tzone, nv).release());
        childOffsets[static_cast<unsigned char>(i)] = 0;
    }
}

UnionConverter::~UnionConverter()
{
    for (size_t i = 0; i < fieldConverters.size(); i++) {
        delete fieldConverters[i];
    }
}

void
UnionConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& unionBatch = dynamic_cast<const orc::UnionVectorBatch&>(batch);
    tags = unionBatch.tags.data();
    offsets = unionBatch.offsets.data();
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->reset(*(unionBatch.children[i]));
    }
}

py::object
UnionConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        return fieldConverters[tags[rowId]]->toPython(offsets[rowId]);
    }
}

void
UnionConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* unionBatch = dynamic_cast<orc::UnionVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        unionBatch->hasNulls = true;
        unionBatch->notNull[rowId] = 0;
    } else {
        for (size_t i = 0; i < fieldConverters.size(); ++i) {
            unsigned char tag = static_cast<unsigned char>(i);
            uint64_t offset = childOffsets[tag];
            try {
                fieldConverters[i]->write(unionBatch->children[i], offset, elem);
                unionBatch->tags[rowId] = tag;
                unionBatch->offsets[rowId] = offset;
                childOffsets[tag] = offset + 1;
                break;
            } catch (py::type_error& err) {
                if (i == fieldConverters.size() - 1) {
                    throw err;
                }
                continue;
            } catch (py::value_error& err) {
                if (i == fieldConverters.size() - 1) {
                    throw err;
                }
                continue;
            }
        }
        unionBatch->notNull[rowId] = 1;
    }
    unionBatch->numElements = rowId + 1;
}

void
UnionConverter::clear()
{
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->clear();
        childOffsets[static_cast<unsigned char>(i)] = 0;
    }
}

StructConverter::StructConverter(const orc::Type& type,
                                 unsigned int kind_,
                                 py::dict conv,
                                 py::object tzone,
                                 py::object nv)
  : Converter(nv)
  , kind(kind_)
{
    for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
        fieldConverters.push_back(
          createConverter(type.getSubtype(i), kind, conv, tzone, nv).release());
        fieldNames.push_back(py::str(type.getFieldName(i)));
    }
}

StructConverter::~StructConverter()
{
    for (size_t i = 0; i < fieldConverters.size(); i++) {
        delete fieldConverters[i];
    }
}

void
StructConverter::reset(const orc::ColumnVectorBatch& batch)
{
    Converter::reset(batch);
    const auto& structBatch = dynamic_cast<const orc::StructVectorBatch&>(batch);
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->reset(*(structBatch.fields[i]));
    }
}

py::object
StructConverter::toPython(uint64_t rowId)
{
    if (hasNulls && !notNull[rowId]) {
        return nullValue;
    } else {
        if (kind == 0) {
            py::tuple result = py::tuple(fieldConverters.size());
            for (size_t i = 0; i < fieldConverters.size(); ++i) {
                result[i] = fieldConverters[i]->toPython(rowId);
            }
            return result;
        } else {
            py::dict result;
            for (size_t i = 0; i < fieldConverters.size(); ++i) {
                result[fieldNames[i]] = fieldConverters[i]->toPython(rowId);
            }
            return result;
        }
    }
}

void
StructConverter::write(orc::ColumnVectorBatch* batch, uint64_t rowId, py::object elem)
{
    auto* structBatch = dynamic_cast<orc::StructVectorBatch*>(batch);
    if (elem.is(nullValue)) {
        structBatch->hasNulls = true;
        structBatch->notNull[rowId] = 0;
        for (size_t i = 0; i < fieldConverters.size(); ++i) {
            if (structBatch->fields[i]->capacity <=
                structBatch->fields[i]->numElements) {
                structBatch->fields[i]->resize(2 * structBatch->fields[i]->capacity);
            }
            fieldConverters[i]->write(structBatch->fields[i], rowId, elem);
        }
    } else {
        if (kind == 0) {
            if (py::isinstance<py::tuple>(elem)) {
                py::tuple tuple(elem);
                for (size_t i = 0; i < fieldConverters.size(); ++i) {
                    if (structBatch->fields[i]->capacity <=
                        structBatch->fields[i]->numElements) {
                        structBatch->fields[i]->resize(
                          2 * structBatch->fields[i]->capacity);
                    }
                    try {
                        fieldConverters[i]->write(
                          structBatch->fields[i], rowId, tuple[i]);
                    } catch (py::type_error& err) {
                        std::stringstream errmsg;
                        errmsg << " at struct field index " << i;
                        throw py::type_error(err.what() + errmsg.str());
                    }
                }
            } else {
                std::stringstream errmsg;
                errmsg << "Item " << (std::string)py::repr(elem)
                       << " is not an instance of tuple";
                throw py::type_error(errmsg.str());
            }
        } else {
            if (py::isinstance<py::dict>(elem)) {
                py::dict dict(elem);
                for (size_t i = 0; i < fieldConverters.size(); ++i) {
                    if (structBatch->fields[i]->capacity <=
                        structBatch->fields[i]->numElements) {
                        structBatch->fields[i]->resize(
                          2 * structBatch->fields[i]->capacity);
                    }
                    try {
                        fieldConverters[i]->write(
                          structBatch->fields[i], rowId, dict[fieldNames[i]]);
                    } catch (py::type_error& err) {
                        std::stringstream errmsg;
                        errmsg << " at struct field name '"
                               << (std::string)fieldNames[i] << "'";
                        throw py::type_error(err.what() + errmsg.str());
                    }
                }
            } else {
                std::stringstream errmsg;
                errmsg << "Item " << (std::string)py::repr(elem)
                       << " is not an instance of dictionary";
                throw py::type_error(errmsg.str());
            }
        }
        structBatch->notNull[rowId] = 1;
    }
    structBatch->numElements = rowId + 1;
}

void
StructConverter::clear()
{
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->clear();
    }
}
