#include <pybind11/stl.h>

#include "PyORCStream.h"
#include "Reader.h"
#include "SearchArgument.h"

using namespace py::literals;

py::dict
createAttributeDict(const orc::Type& orcType)
{
    py::dict result;
    for (std::string key : orcType.getAttributeKeys()) {
        result[key.c_str()] = py::str(orcType.getAttributeValue(key).c_str());
    }
    return result;
}

py::object
createTypeDescription(const orc::Type& orcType)
{
    py::object typeModule = py::module::import("pyorc.typedescription");
    int kind = static_cast<int>(orcType.getKind());
    py::object attrDict = createAttributeDict(orcType);
    switch (kind) {
        case orc::BOOLEAN: {
            py::object typeDesc = typeModule.attr("Boolean")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::BYTE: {
            py::object typeDesc = typeModule.attr("TinyInt")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::SHORT: {
            py::object typeDesc = typeModule.attr("SmallInt")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::INT: {
            py::object typeDesc = typeModule.attr("Int")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::LONG: {
            py::object typeDesc = typeModule.attr("BigInt")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::FLOAT: {
            py::object typeDesc = typeModule.attr("Float")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::DOUBLE: {
            py::object typeDesc = typeModule.attr("Double")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::STRING: {
            py::object typeDesc = typeModule.attr("String")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::BINARY: {
            py::object typeDesc = typeModule.attr("Binary")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::TIMESTAMP: {
            py::object typeDesc = typeModule.attr("Timestamp")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::TIMESTAMP_INSTANT: {
            py::object typeDesc = typeModule.attr("TimestampInstant")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::DATE: {
            py::object typeDesc = typeModule.attr("Date")();
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::CHAR: {
            py::object typeDesc =
              typeModule.attr("Char")(py::cast(orcType.getMaximumLength()));
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::VARCHAR: {
            py::object typeDesc =
              typeModule.attr("VarChar")(py::cast(orcType.getMaximumLength()));
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::DECIMAL: {
            py::object typeDesc = typeModule.attr("Decimal")(
              "precision"_a = py::cast(orcType.getPrecision()),
              "scale"_a = py::cast(orcType.getScale()));
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::LIST: {
            py::object typeDesc =
              typeModule.attr("Array")(createTypeDescription(*orcType.getSubtype(0)));
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::MAP: {
            py::object typeDesc = typeModule.attr("Map")(
              "key"_a = createTypeDescription(*orcType.getSubtype(0)),
              "value"_a = createTypeDescription(*orcType.getSubtype(1)));
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::UNION: {
            py::tuple args(orcType.getSubtypeCount());
            for (size_t i = 0; i < orcType.getSubtypeCount(); ++i) {
                args[i] = createTypeDescription(*orcType.getSubtype(i));
            }
            py::object typeDesc = typeModule.attr("Union")(*args);
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        case orc::STRUCT: {
            py::dict fields;
            for (size_t i = 0; i < orcType.getSubtypeCount(); ++i) {
                auto key = orcType.getFieldName(i);
                fields[key.c_str()] = createTypeDescription(*orcType.getSubtype(i));
            }
            py::object typeDesc = typeModule.attr("Struct")(**fields);
            typeDesc.attr("set_attributes")(attrDict);
            return typeDesc;
        }
        default:
            throw py::type_error("Invalid TypeKind");
    }
}

py::object
ORCFileLikeObject::next()
{
    while (true) {
        if (batchItem == 0) {
            if (!rowReader->next(*batch)) {
                throw py::stop_iteration();
            }
            converter->reset(*batch);
        }
        if (batchItem < batch->numElements) {
            py::object val = converter->toPython(batchItem);
            ++batchItem;
            ++currentRow;
            return val;
        } else {
            batchItem = 0;
        }
    }
}

py::list
ORCFileLikeObject::read(int64_t num)
{
    int64_t i = 0;
    py::list res;
    if (num < -1) {
        throw py::value_error("Read length must be positive or -1");
    }
    try {
        while (true) {
            if (num != -1 && i == num) {
                return res;
            }
            res.append(this->next());
            ++i;
        }
    } catch (py::stop_iteration&) {
        return res;
    }
}

uint64_t
ORCFileLikeObject::seek(int64_t row, uint16_t whence)
{
    uint64_t start = 0;
    switch (whence) {
        case 0:
            start = firstRowOfStripe;
            if (row < 0) {
                throw py::value_error("Invalid value for row");
            }
            break;
        case 1:
            start = currentRow + firstRowOfStripe;
            break;
        case 2:
            start = this->len() + firstRowOfStripe;
            break;
        default:
            throw py::value_error("Invalid value for whence");
            break;
    }
    rowReader->seekToRow(start + row);
    batchItem = 0;
    currentRow = rowReader->getRowNumber() - firstRowOfStripe;
    return currentRow;
}

const orc::Type*
ORCFileLikeObject::findColumnType(const orc::Type* type, uint64_t columnIndex) const
{
    if (type->getColumnId() == columnIndex) {
        return type;
    } else {
        for (size_t i = 0; i < type->getSubtypeCount(); ++i) {
            auto* subtype = type->getSubtype(i);
            if (subtype->getColumnId() <= columnIndex &&
                subtype->getMaximumColumnId() >= columnIndex) {
                return ORCFileLikeObject::findColumnType(subtype, columnIndex);
            }
        }
        throw py::index_error("column not found");
    }
}

py::object
ORCFileLikeObject::convertTimestampMillis(int64_t millisec) const
{
    py::object idx(py::int_(static_cast<int>(orc::TIMESTAMP)));
    py::object from_orc = convDict[idx].attr("from_orc");
    int64_t seconds = millisec / 1000;
    int64_t nanosecs = std::abs(millisec % 1000) * 1000 * 1000;
    return from_orc(seconds, nanosecs, timezoneInfo);
}

py::dict
ORCFileLikeObject::buildStatistics(const orc::Type* type,
                                   const orc::ColumnStatistics* stats) const
{
    py::dict result;
    int64_t typeKind = static_cast<int64_t>(type->getKind());
    result["kind"] = typeKind;
    result["has_null"] = py::cast(stats->hasNull());
    result["number_of_values"] = py::cast(stats->getNumberOfValues());
    switch (typeKind) {
        case orc::BOOLEAN: {
            auto* boolStat = dynamic_cast<const orc::BooleanColumnStatistics*>(stats);
            if (boolStat->hasCount()) {
                result["false_count"] = py::cast(boolStat->getFalseCount());
                result["true_count"] = py::cast(boolStat->getTrueCount());
            }
            return result;
        }
        case orc::BYTE:
        case orc::INT:
        case orc::LONG:
        case orc::SHORT: {
            auto* intStat = dynamic_cast<const orc::IntegerColumnStatistics*>(stats);
            if (intStat->hasMinimum()) {
                result["minimum"] = py::cast(intStat->getMinimum());
            }
            if (intStat->hasMaximum()) {
                result["maximum"] = py::cast(intStat->getMaximum());
            }
            if (intStat->hasSum()) {
                result["sum"] = py::cast(intStat->getSum());
            }
            return result;
        }
        case orc::STRUCT:
        case orc::MAP:
        case orc::LIST:
        case orc::UNION:
            return result;
        case orc::FLOAT:
        case orc::DOUBLE: {
            auto* doubleStat = dynamic_cast<const orc::DoubleColumnStatistics*>(stats);
            if (doubleStat->hasMinimum()) {
                result["minimum"] = py::cast(doubleStat->getMinimum());
            }
            if (doubleStat->hasMaximum()) {
                result["maximum"] = py::cast(doubleStat->getMaximum());
            }
            if (doubleStat->hasSum()) {
                result["sum"] = py::cast(doubleStat->getSum());
            }
            return result;
        }
        case orc::BINARY: {
            auto* binaryStat = dynamic_cast<const orc::BinaryColumnStatistics*>(stats);
            if (binaryStat->hasTotalLength()) {
                result["total_length"] = py::cast(binaryStat->getTotalLength());
            }
            return result;
        }
        case orc::STRING:
        case orc::CHAR:
        case orc::VARCHAR: {
            auto* strStat = dynamic_cast<const orc::StringColumnStatistics*>(stats);
            if (strStat->hasMinimum()) {
                result["minimum"] = py::cast(strStat->getMinimum());
            }
            if (strStat->hasMaximum()) {
                result["maximum"] = py::cast(strStat->getMaximum());
            }
            if (strStat->hasTotalLength()) {
                result["total_length"] = py::cast(strStat->getTotalLength());
            }
            return result;
        }
        case orc::DATE: {
            auto* dateStat = dynamic_cast<const orc::DateColumnStatistics*>(stats);
            py::object idx(py::int_(static_cast<int>(orc::DATE)));
            py::object from_orc = convDict[idx].attr("from_orc");
            if (dateStat->hasMinimum()) {
                result["minimum"] = from_orc(dateStat->getMinimum());
            }
            if (dateStat->hasMaximum()) {
                result["maximum"] = from_orc(dateStat->getMaximum());
            }
            return result;
        }
        case orc::TIMESTAMP:
        case orc::TIMESTAMP_INSTANT: {
            auto* timeStat = dynamic_cast<const orc::TimestampColumnStatistics*>(stats);
            if (timeStat->hasMinimum()) {
                result["minimum"] = convertTimestampMillis(timeStat->getMinimum());
            }
            if (timeStat->hasMaximum()) {
                result["maximum"] = convertTimestampMillis(timeStat->getMaximum());
            }
            if (timeStat->hasLowerBound()) {
                result["lower_bound"] =
                  convertTimestampMillis(timeStat->getLowerBound());
            }
            if (timeStat->hasUpperBound()) {
                result["upper_bound"] =
                  convertTimestampMillis(timeStat->getUpperBound());
            }
            return result;
        }
        case orc::DECIMAL: {
            auto* decStat = dynamic_cast<const orc::DecimalColumnStatistics*>(stats);
            py::object idx(py::int_(static_cast<int>(orc::DECIMAL)));
            py::object from_orc = convDict[idx].attr("from_orc");
            if (decStat->hasMinimum()) {
                result["minimum"] = from_orc(decStat->getMinimum().toString());
            }
            if (decStat->hasMaximum()) {
                result["maximum"] = from_orc(decStat->getMaximum().toString());
            }
            if (decStat->hasSum()) {
                result["sum"] = from_orc(decStat->getSum().toString());
            }
            return result;
        }
        default:
            return result;
    }
}

Reader::Reader(py::object fileo,
               uint64_t batch_size,
               std::list<uint64_t> col_indices,
               std::list<std::string> col_names,
               py::object tzone,
               unsigned int struct_repr,
               py::object conv,
               py::object predicate,
               py::object null_value)
{
    orc::ReaderOptions readerOpts;
    batchItem = 0;
    currentRow = 0;
    firstRowOfStripe = 0;
    structKind = struct_repr;
    nullValue = null_value;
    if (!col_indices.empty() && !col_names.empty()) {
        throw py::value_error(
          "Either col_indices or col_names can be set to select columns");
    }
    if (!col_indices.empty()) {
        rowReaderOpts = rowReaderOpts.include(col_indices);
    }
    if (!col_names.empty()) {
        rowReaderOpts = rowReaderOpts.include(col_names);
    }
    if (!tzone.is_none()) {
        std::string tzKey = py::cast<std::string>(tzone.attr("key"));
        rowReaderOpts = rowReaderOpts.setTimezoneName(tzKey);
    }
    timezoneInfo = tzone;
    if (conv.is_none()) {
        py::dict defaultConv =
          py::module::import("pyorc.converters").attr("DEFAULT_CONVERTERS");
        convDict = py::dict(defaultConv);
    } else {
        convDict = conv;
    }
    if (!predicate.is_none()) {
        rowReaderOpts = rowReaderOpts.searchArgument(
          std::move(createSearchArgument(predicate, convDict, timezoneInfo)));
    }
    reader = orc::createReader(
      std::unique_ptr<orc::InputStream>(new PyORCInputStream(fileo)), readerOpts);
    try {
        batchSize = batch_size;
        rowReader = reader->createRowReader(rowReaderOpts);
        batch = rowReader->createRowBatch(batchSize);
        converter = createConverter(
          &rowReader->getSelectedType(), structKind, convDict, timezoneInfo, nullValue);
    } catch (orc::ParseError& err) {
        throw py::value_error(err.what());
    }
}

py::dict
Reader::bytesLengths() const
{
    py::dict res;
    res["content_length"] = reader->getContentLength();
    res["file_footer_length"] = reader->getFileFooterLength();
    res["file_postscript_length"] = reader->getFilePostscriptLength();
    res["file_length"] = reader->getFileLength();
    res["stripe_statistics_length"] = reader->getStripeStatisticsLength();
    return res;
}

uint64_t
Reader::compression() const
{
    return static_cast<uint64_t>(reader->getCompression());
}

uint64_t
Reader::compressionBlockSize() const
{
    return reader->getCompressionSize();
}

uint64_t
Reader::rowIndexStride() const
{
    return reader->getRowIndexStride();
}

py::tuple
Reader::formatVersion() const
{
    py::tuple res(2);
    orc::FileVersion ver = reader->getFormatVersion();
    res[0] = py::cast(ver.getMajor());
    res[1] = py::cast(ver.getMinor());
    return res;
}

uint64_t
Reader::len() const
{
    return reader->getNumberOfRows();
}

uint64_t
Reader::numberOfStripes() const
{
    return reader->getNumberOfStripes();
}

uint32_t
Reader::writerId() const
{
    return reader->getWriterIdValue();
}

uint32_t
Reader::writerVersion() const
{
    return reader->getWriterVersion();
}

std::string
Reader::softwareVersion() const
{
    return reader->getSoftwareVersion();
}

std::unique_ptr<Stripe>
Reader::readStripe(uint64_t idx)
{
    if (idx >= reader->getNumberOfStripes()) {
        throw py::index_error("stripe index out of range");
    }
    return std::unique_ptr<Stripe>(new Stripe(*this, idx, reader->getStripe(idx)));
}

py::object
Reader::schema()
{
    return createTypeDescription(reader->getType());
}

py::object
Reader::selectedSchema()
{
    return createTypeDescription(rowReader->getSelectedType());
}

py::tuple
Reader::statistics(uint64_t columnIndex)
{
    try {
        py::tuple result = py::tuple(1);
        std::unique_ptr<orc::ColumnStatistics> stats =
          reader->getColumnStatistics(columnIndex);
        result[0] = this->buildStatistics(
          this->findColumnType(&rowReader->getSelectedType(), columnIndex),
          stats.get());
        return result;
    } catch (std::logic_error& err) {
        throw py::index_error(err.what());
    }
}

py::dict
Reader::userMetadata()
{
    py::dict result;
    for (std::string key : reader->getMetadataKeys()) {
        result[key.c_str()] = py::bytes(reader->getMetadataValue(key));
    }
    return result;
}

Stripe::Stripe(const Reader& reader_,
               uint64_t idx,
               std::unique_ptr<orc::StripeInformation> stripe)
  : reader(reader_)
{
    batchItem = 0;
    currentRow = 0;
    stripeIndex = idx;
    stripeInfo = std::move(stripe);
    convDict = reader.getConverterDict();
    timezoneInfo = reader.getTimeZoneInfo();
    rowReaderOpts = reader.getRowReaderOptions();
    rowReaderOpts =
      rowReaderOpts.range(stripeInfo->getOffset(), stripeInfo->getLength());
    rowReader = reader.getORCReader().createRowReader(rowReaderOpts);
    batch = rowReader->createRowBatch(reader.getBatchSize());
    converter = createConverter(&rowReader->getSelectedType(),
                                reader.getStructKind(),
                                convDict,
                                timezoneInfo,
                                reader.getNullValue());
    firstRowOfStripe = rowReader->getRowNumber() + 1;
}

py::tuple
Stripe::bloomFilterColumns()
{
    int64_t idx = 0;
    std::set<uint32_t> empty = {};
    std::map<uint32_t, orc::BloomFilterIndex> bfCols =
      reader.getORCReader().getBloomFilters(stripeIndex, empty);
    py::tuple result(bfCols.size());
    for (auto const& col : bfCols) {
        result[idx] = py::cast(col.first);
        ++idx;
    }
    return result;
}

uint64_t
Stripe::len() const
{
    return stripeInfo->getNumberOfRows();
}

uint64_t
Stripe::length() const
{
    return stripeInfo->getLength();
}

uint64_t
Stripe::offset() const
{
    return stripeInfo->getOffset();
}

py::tuple
Stripe::statistics(uint64_t columnIndex)
{
    if (columnIndex < 0 ||
        columnIndex > rowReader->getSelectedType().getMaximumColumnId()) {
        throw py::index_error("column index out of range");
    }
    std::unique_ptr<orc::StripeStatistics> stripeStats =
      reader.getORCReader().getStripeStatistics(stripeIndex);
    uint32_t num = stripeStats->getNumberOfRowIndexStats(columnIndex);
    py::tuple result = py::tuple(num);
    for (uint32_t i = 0; i < num; ++i) {
        const orc::ColumnStatistics* stats =
          stripeStats->getRowIndexStatistics(columnIndex, i);
        result[i] = this->buildStatistics(
          this->findColumnType(&rowReader->getSelectedType(), columnIndex), stats);
    }
    return result;
}

std::string
Stripe::writerTimezone()
{
    return stripeInfo->getWriterTimezone();
}
