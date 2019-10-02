#include <cmath>
#include <sstream>

#include "Converter.h"

namespace py = pybind11;

class BoolConverter: public Converter {
private:
    const int64_t* data;
public:
    BoolConverter(): Converter(), data(nullptr) {}
    ~BoolConverter() override {}
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class LongConverter: public Converter {
private:
    const int64_t* data;
public:
    LongConverter(): Converter(), data(nullptr) {}
    ~LongConverter() override {}
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class DoubleConverter: public Converter {
private:
    const double* data;
public:
    DoubleConverter(): Converter(), data(nullptr) {}
    ~DoubleConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class StringConverter: public Converter {
private:
    const char* const * data;
    const int64_t* length;
    std::vector<py::object> buffer;
public:
    StringConverter(): Converter(), data(nullptr), length(nullptr) {}
    ~StringConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class BinaryConverter: public Converter {
private:
    const char* const * data;
    const int64_t* length;
    std::vector<py::object> buffer;
public:
    BinaryConverter(): Converter(), data(nullptr), length(nullptr) {}
    ~BinaryConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class TimestampConverter: public Converter {
private:
    const int64_t* seconds;
    const int64_t* nanoseconds;
    py::object datetime;
public:
    TimestampConverter();
    virtual ~TimestampConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class DateConverter: public Converter {
private:
    const int64_t* data;
    py::object date;
    py::object epochDate;
public:
    DateConverter();
    virtual ~DateConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class Decimal64Converter: public Converter {
private:
    const int64_t* data;
    int32_t scale;
    py::object decimal;
public:
    Decimal64Converter();
    virtual ~Decimal64Converter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    //void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class Decimal128Converter: public Converter {
private:
    const orc::Int128* data;
    int32_t scale;
    py::object decimal;
public:
    Decimal128Converter();
    virtual ~Decimal128Converter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    //void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
};

class ListConverter: public Converter {
private:
    const int64_t* offsets;
    std::unique_ptr<Converter> elementConverter;
public:
    ListConverter(const orc::Type& type);
    virtual ~ListConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class MapConverter: public Converter {
private:
    const int64_t* offsets;
    std::unique_ptr<Converter> keyConverter;
    std::unique_ptr<Converter> elementConverter;
public:
    MapConverter(const orc::Type& type);
    virtual ~MapConverter() override {};
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

class StructConverter: public Converter {
private:
    std::vector<Converter*> fieldConverters;
    std::vector<py::str> fieldNames;
public:
    StructConverter(const orc::Type& type);
    virtual ~StructConverter() override;
    py::object convert(uint64_t rowId) override;
    void reset(const orc::ColumnVectorBatch& batch) override;
    void write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) override;
    void clear() override;
};

std::unique_ptr<Converter> createConverter(const orc::Type* type) {
    Converter *result = nullptr;
    if (type == nullptr) {
        result = nullptr;
    } else {
        switch(static_cast<int64_t>(type->getKind())) {
        case orc::BOOLEAN:
            result = new BoolConverter();
            break;
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
            result = new LongConverter();
            break;
        case orc::FLOAT:
        case orc::DOUBLE:
            result = new DoubleConverter();
            break;
        case orc::STRING:
        case orc::VARCHAR:
        case orc::CHAR:
            result = new StringConverter();
            break;
        case orc::BINARY:
            result = new BinaryConverter();
            break;
        case orc::TIMESTAMP:
            result = new TimestampConverter();
            break;
        case orc::LIST:
            result = new ListConverter(*type);
            break;
        case orc::MAP:
            result = new MapConverter(*type);
            break;
        case orc::STRUCT:
            result = new StructConverter(*type);
            break;
        case orc::DECIMAL:
            if (type->getPrecision() == 0 || type->getPrecision() > 18) {
                result = new Decimal128Converter();
            } else {
                result = new Decimal64Converter();
            }
            break;
        case orc::DATE:
            result = new DateConverter();
            break;
        case orc::UNION:
            result = nullptr; //new UnionColumnPrinter(buffer, *type);
            break;
        default:
            throw std::logic_error("unknown batch type");
      }
    }
    return std::unique_ptr<Converter>(result);
}

void Converter::reset(const orc::ColumnVectorBatch& batch) {
    hasNulls = batch.hasNulls;
    if (hasNulls) {
        notNull = batch.notNull.data();
    } else {
        notNull = nullptr;
    }
}

void Converter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) { std::cout << "NOPE\n";}

void BoolConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}
 
py::object BoolConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return py::bool_(data[rowId]);
    }
}

void BoolConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch);
    if (elem.is(py::none())) {
        longBatch->hasNulls = true;
        longBatch->notNull[rowId] = 0;
    } else {
        longBatch->data[rowId] = py::cast<int64_t>(elem);
        longBatch->notNull[rowId] = 1;
    }
    longBatch->numElements = rowId + 1;
}

void LongConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}

py::object LongConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return py::cast(data[rowId]);
    }
}

void LongConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *longBatch = dynamic_cast<orc::LongVectorBatch *>(batch);
    if (elem.is(py::none())) {
        longBatch->hasNulls = true;
        longBatch->notNull[rowId] = 0;
    } else {
        longBatch->data[rowId] = py::cast<int64_t>(elem);
        longBatch->notNull[rowId] = 1;
    }
    longBatch->numElements = rowId + 1;
}

void DoubleConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    data = dynamic_cast<const orc::DoubleVectorBatch&>(batch).data.data();
}

py::object DoubleConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return py::cast(data[rowId]);
    }
}

void DoubleConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *doubleBatch = dynamic_cast<orc::DoubleVectorBatch *>(batch);
    if (elem.is(py::none())) {
        doubleBatch->hasNulls = true;
        doubleBatch->notNull[rowId] = 0;
    } else {
        doubleBatch->data[rowId] = py::cast<double>(elem);
        doubleBatch->notNull[rowId] = 1;
    }
    doubleBatch->numElements = rowId + 1;
}

void StringConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& strBatch = dynamic_cast<const orc::StringVectorBatch&>(batch);
    data = strBatch.data.data();
    length = strBatch.length.data();
}

py::object StringConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return py::str(data[rowId], static_cast<size_t>(length[rowId]));
    }
}

void StringConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *strBatch = dynamic_cast<orc::StringVectorBatch *>(batch);
    if (elem.is(py::none())) {
        strBatch->hasNulls = true;
        strBatch->notNull[rowId] = 0;
    } else {
        Py_ssize_t length = 0;
        char *src = PyUnicode_AsUTF8AndSize(elem.ptr(), &length);
        if (src == nullptr) {
            throw py::error_already_set();
        }
        buffer.push_back(elem);
        strBatch->data[rowId] = src;
        strBatch->length[rowId] = static_cast<int64_t>(length);
        strBatch->notNull[rowId] = 1;
    }
    strBatch->numElements = rowId + 1;
}

void StringConverter::clear() {
    buffer.clear();
}

void BinaryConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& strBatch = dynamic_cast<const orc::StringVectorBatch&>(batch);
    data = strBatch.data.data();
    length = strBatch.length.data();
}

py::object BinaryConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return py::bytes(data[rowId], static_cast<size_t>(length[rowId]));
    }
}

void BinaryConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    char *src = nullptr;
    auto *bytesBatch = dynamic_cast<orc::StringVectorBatch *>(batch);
    if (elem.is(py::none())) {
        bytesBatch->hasNulls = true;
        bytesBatch->notNull[rowId] = 0;
    } else {
        Py_ssize_t length = 0;
        int rc = PyBytes_AsStringAndSize(elem.ptr(), &src, &length);
        if (rc == -1) {
            throw py::error_already_set();
        }
        buffer.push_back(elem);
        bytesBatch->data[rowId] = src;
        bytesBatch->length[rowId] = static_cast<int64_t>(length);
        bytesBatch->notNull[rowId] = 1;
    }
    bytesBatch->numElements = rowId + 1;
}

void BinaryConverter::clear() {
    buffer.clear();
}

TimestampConverter::TimestampConverter(): Converter(), seconds(nullptr), nanoseconds(nullptr) {
    py::object dt = py::module::import("datetime").attr("datetime");
    datetime = dt.attr("fromtimestamp");
}

void TimestampConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& ts = dynamic_cast<const orc::TimestampVectorBatch&>(batch);
    seconds = ts.data.data();
    nanoseconds = ts.nanoseconds.data();
}

py::object TimestampConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        py::object date = datetime(seconds[rowId]);
        py::object replace(date.attr("replace"));
        return replace(py::arg("microsecond") = (nanoseconds[rowId] / 1000));
    }
}

void TimestampConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *tsBatch = dynamic_cast<orc::TimestampVectorBatch *>(batch);
    if (elem.is(py::none())) {
        tsBatch->hasNulls = true;
        tsBatch->notNull[rowId] = 0;
    } else {
        py::object touts(elem.attr("timestamp"));
        py::object microseconds(elem.attr("microsecond"));
        tsBatch->data[rowId] = static_cast<int64_t>(py::cast<double>(touts()));
        tsBatch->nanoseconds[rowId] = py::cast<int64_t>(microseconds) * 1000;
        tsBatch->notNull[rowId] = 1;
    }
    tsBatch->numElements = rowId + 1;
}

DateConverter::DateConverter(): Converter(), data(nullptr) {
    date = py::module::import("datetime").attr("date");
    epochDate = date(1970, 1, 1);
}

void DateConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.data();
}

py::object DateConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        py::object pyfromts(date.attr("fromtimestamp"));
        return pyfromts(data[rowId] * 24 * 60 * 60);
    }
}

void DateConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *dBatch = dynamic_cast<orc::LongVectorBatch *>(batch);
    if (elem.is(py::none())) {
        dBatch->hasNulls = true;
        dBatch->notNull[rowId] = 0;
    } else {
        py::object delta = elem - epochDate;
        dBatch->data[rowId] = py::cast<int64_t>(delta.attr("days"));
        dBatch->notNull[rowId] = 1;
    }
    dBatch->numElements = rowId + 1;
}

Decimal64Converter::Decimal64Converter(): Converter(), data(nullptr) {
    decimal = py::module::import("decimal").attr("Decimal");
}

void Decimal64Converter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& decBatch = dynamic_cast<const orc::Decimal64VectorBatch&>(batch);
    data = decBatch.values.data();
    scale = decBatch.scale;
}

std::string toDecimalString(int64_t value, int32_t scale) {
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
        return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." + str.substr(static_cast<size_t>(len - scale), static_cast<size_t>(scale));
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

py::object Decimal64Converter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return decimal(toDecimalString(data[rowId], scale));
    }
}

Decimal128Converter::Decimal128Converter(): Converter(), data(nullptr) {
    decimal = py::module::import("decimal").attr("Decimal");
}

void Decimal128Converter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& decBatch = dynamic_cast<const orc::Decimal128VectorBatch&>(batch);
    data = decBatch.values.data();
    scale = decBatch.scale;
}

py::object Decimal128Converter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        return decimal(data[rowId].toDecimalString(scale));
    }
}

ListConverter::ListConverter(const orc::Type& type): Converter(), offsets(nullptr) {
    elementConverter = createConverter(type.getSubtype(0));
}

void ListConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& listBatch = dynamic_cast<const orc::ListVectorBatch&>(batch);
    offsets = listBatch.offsets.data();
    elementConverter->reset(*listBatch.elements);
}

py::object ListConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        py::list result;
        for (int64_t i = offsets[rowId]; i < offsets[rowId + 1]; ++i) {
            result.append(elementConverter->convert(static_cast<uint64_t>(i)));
        }
        return result;
    }
}

void ListConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    size_t size = 0;
    auto *listBatch = dynamic_cast<orc::ListVectorBatch *>(batch);
    listBatch->offsets[0] = 0;
    uint64_t offset = static_cast<uint64_t>(listBatch->offsets[rowId]);
    if (elem.is(py::none())) {
        listBatch->hasNulls = true;
        listBatch->notNull[rowId] = 0;
    } else {
        py::list list(elem);
        size = list.size();
        if (listBatch->elements->capacity > offset + size) {
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

void ListConverter::clear() {
    elementConverter->clear();
}

MapConverter::MapConverter(const orc::Type& type): Converter(), offsets(nullptr) {
    keyConverter = createConverter(type.getSubtype(0));
    elementConverter = createConverter(type.getSubtype(1));
}

void MapConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& mapBatch = dynamic_cast<const orc::MapVectorBatch&>(batch);
    offsets = mapBatch.offsets.data();
    keyConverter->reset(*mapBatch.keys);
    elementConverter->reset(*mapBatch.elements);
}

py::object MapConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        py::dict result;
        for (int64_t i = offsets[rowId]; i < offsets[rowId+1]; ++i) {
            py::object key = keyConverter->convert(static_cast<uint64_t>(i));
            result[key] = elementConverter->convert(static_cast<uint64_t>(i));
        }
        return result;
    }
}

void MapConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    uint64_t cnt = 0;
    auto *mapBatch = dynamic_cast<orc::MapVectorBatch *>(batch);
    mapBatch->offsets[0] = 0;
    uint64_t offset = static_cast<uint64_t>(mapBatch->offsets[rowId]);
    if (elem.is(py::none())) {
        mapBatch->hasNulls = true;
        mapBatch->notNull[rowId] = 0;
    } else {
        py::dict dict(elem);
        size_t size = dict.size();
        if (mapBatch->keys->capacity > offset + size) {
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

void MapConverter::clear() {
    keyConverter->clear();
    elementConverter->clear();
}

StructConverter::StructConverter(const orc::Type& type) {
    for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
        fieldConverters.push_back(createConverter(type.getSubtype(i)).release());
        fieldNames.push_back(py::str(type.getFieldName(i)));
    }
}

StructConverter::~StructConverter() {
    for (size_t i = 0; i < fieldConverters.size(); i++) {
        delete fieldConverters[i];
    }
}

void StructConverter::reset(const orc::ColumnVectorBatch& batch) {
    Converter::reset(batch);
    const auto& structBatch = dynamic_cast<const orc::StructVectorBatch&>(batch);
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->reset(*(structBatch.fields[i]));
    }
}

py::object StructConverter::convert(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
        return py::none();
    } else {
        //py::tuple result = py::tuple(fieldConverters.size());
        py::dict result;
        for (size_t i = 0; i < fieldConverters.size(); ++i) {
            //result[i] = fieldConverters[i]->convert(rowId);
            result[fieldNames[i]] = fieldConverters[i]->convert(rowId);
        }
        return result;
    }
}

void StructConverter::write(orc::ColumnVectorBatch *batch, uint64_t rowId, py::object elem) {
    auto *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch);
    if (elem.is(py::none())) {
        structBatch->hasNulls = true;
        structBatch->notNull[rowId] = 0;
        for (size_t i = 0; i < fieldConverters.size(); ++i) {
            fieldConverters[i]->write(structBatch->fields[i], rowId, elem);
        }
    } else {
        py::dict dict(elem);
        for (size_t i = 0; i < fieldConverters.size(); ++i) {
            fieldConverters[i]->write(structBatch->fields[i], rowId, dict[fieldNames[i]]);
        }
        structBatch->notNull[rowId] = 1;
    }
    structBatch->numElements = rowId + 1;
}

void StructConverter::clear() {
    for (size_t i = 0; i < fieldConverters.size(); ++i) {
        fieldConverters[i]->clear();
    }
}
