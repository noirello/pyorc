#include "Column.h"

#include <cstdlib>

Column::Column(const Stripe& stripe_,
               uint64_t num,
               std::map<uint32_t, orc::BloomFilterIndex> bloomFilters)
  : columnIndex(num)
  , stripe(stripe_)
{
    batchItem = 0;
    currentRow = 0;
    try {
        stats = stripe.getReader().getORCReader().getColumnStatistics(columnIndex);
    } catch (std::logic_error& err) {
        throw py::index_error(err.what());
    }
    if (!bloomFilters.empty()) {
        bloomFilter = std::unique_ptr<orc::BloomFilterIndex>(
          new orc::BloomFilterIndex(bloomFilters[static_cast<uint32_t>(columnIndex)]));
    } else {
        bloomFilter = std::unique_ptr<orc::BloomFilterIndex>(nullptr);
    }
    rowReaderOpts = stripe.getRowReaderOptions();
    rowReader = stripe.getReader().getORCReader().createRowReader(rowReaderOpts);
    batch = rowReader->createRowBatch(stripe.getReader().getBatchSize());
    const orc::Type* type = this->findColumnType(&rowReader->getSelectedType());
    converter = createConverter(
      type, stripe.getReader().getStructKind(), stripe.getReader().getConverters());
    firstRowOfStripe = rowReader->getRowNumber() + 1;
    typeKind = static_cast<int64_t>(type->getKind());
    selectedBatch = this->selectBatch(rowReader->getSelectedType(), batch.get());
}

void
Column::jumpToPosition(int64_t row, uint64_t batch)
{
    rowReader->seekToRow(firstRowOfStripe + row);
    batchItem = batch;
    currentRow = rowReader->getRowNumber() - firstRowOfStripe;
}

bool
Column::testBloomFilter(py::object item)
{
    char* data = nullptr;
    Py_ssize_t length = 0;
    switch (typeKind) {
        case orc::BOOLEAN:
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG: {
            int64_t longItem = py::cast<int64_t>(item);
            for (auto entry : bloomFilter->entries) {
                if (entry->testLong(longItem) == true) {
                    return true;
                }
            }
            break;
        }
        case orc::FLOAT:
        case orc::DOUBLE: {
            double floatItem = py::cast<double>(item);
            for (auto entry : bloomFilter->entries) {
                if (entry->testDouble(floatItem) == true) {
                    return true;
                }
            }
            break;
        }
        case orc::STRING:
        case orc::VARCHAR:
        case orc::CHAR:
            data = const_cast<char*>(PyUnicode_AsUTF8AndSize(item.ptr(), &length));
            for (auto entry : bloomFilter->entries) {
                if (entry->testBytes(data, static_cast<int64_t>(length)) == true) {
                    return true;
                }
            }
            break;
        case orc::DATE: {
            py::object idx(py::int_(static_cast<int>(orc::DATE)));
            py::object to_orc = stripe.getReader().getConverters()[idx].attr("to_orc");
            int64_t days = py::cast<int64_t>(to_orc(item));
            for (auto entry : bloomFilter->entries) {
                if (entry->testLong(days) == true) {
                    return true;
                }
            }
            break;
        }
        case orc::TIMESTAMP: {
            py::object idx(py::int_(static_cast<int>(orc::TIMESTAMP)));
            py::object to_orc = stripe.getReader().getConverters()[idx].attr("to_orc");
            py::tuple res = to_orc(item);
            int64_t millis =
              py::cast<int64_t>(res[0]) * 1000 + py::cast<int64_t>(res[1]) / 1000000;
            for (auto entry : bloomFilter->entries) {
                if (entry->testLong(millis) == true) {
                    return true;
                }
            }
            break;
        }
        case orc::DECIMAL: {
            const orc::Type* type = this->findColumnType(&rowReader->getSelectedType());
            py::object idx(py::int_(static_cast<int>(orc::DECIMAL)));
            py::object to_orc = stripe.getReader().getConverters()[idx].attr("to_orc");
            std::string res =
              orc::Decimal(orc::Int128(py::cast<std::string>(py::str(
                             to_orc(type->getPrecision(), type->getScale(), item)))),
                           type->getScale())
                .toString();
            for (auto entry : bloomFilter->entries) {
                if (entry->testBytes(res.c_str(), res.size()) == true) {
                    return true;
                }
            }
            break;
        }
        default:
            return true;
    }
    return false;
}

bool
Column::contains(py::object item)
{
    uint64_t tmpRowPos = currentRow;
    uint64_t tmpbatchPos = batchItem;
    if (bloomFilter) {
        if (!testBloomFilter(item)) {
            return false;
        }
    }
    try {
        /* Start the searching from the first item, regardless the current position. */
        this->jumpToPosition(0, 0);
        while (true) {
            if (item.equal(this->next())) {
                this->jumpToPosition(tmpRowPos, tmpbatchPos);
                return true;
            }
        }
    } catch (py::stop_iteration) {
        this->jumpToPosition(tmpRowPos, tmpbatchPos);
        return false;
    }
}

py::object
Column::convertTimestampMillis(int64_t millisec)
{
    py::object idx(py::int_(static_cast<int>(orc::TIMESTAMP)));
    py::object from_orc = stripe.getReader().getConverters()[idx].attr("from_orc");
    int64_t seconds = millisec / 1000;
    int64_t nanosecs = std::abs(millisec % 1000) * 1000 * 1000;
    return from_orc(seconds, nanosecs);
}

py::object
Column::statistics()
{
    py::dict result;
    py::object enumKind = py::module::import("pyorc.enums").attr("TypeKind");
    result["kind"] = enumKind(typeKind);
    result["has_null"] = py::cast(stats->hasNull());
    result["number_of_values"] = py::cast(stats->getNumberOfValues());
    switch (typeKind) {
        case orc::BOOLEAN: {
            auto& boolStat = dynamic_cast<orc::BooleanColumnStatistics&>(*stats);
            if (boolStat.hasCount()) {
                result["false_count"] = py::cast(boolStat.getFalseCount());
                result["true_count"] = py::cast(boolStat.getTrueCount());
            }
            return result;
        }
        case orc::BYTE:
        case orc::INT:
        case orc::LONG:
        case orc::SHORT: {
            auto* intStat = dynamic_cast<orc::IntegerColumnStatistics*>(stats.get());
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
            auto* doubleStat = dynamic_cast<orc::DoubleColumnStatistics*>(stats.get());
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
            auto* binaryStat = dynamic_cast<orc::BinaryColumnStatistics*>(stats.get());
            if (binaryStat->hasTotalLength()) {
                result["total_length"] = py::cast(binaryStat->getTotalLength());
            }
            return result;
        }
        case orc::STRING:
        case orc::CHAR:
        case orc::VARCHAR: {
            auto* strStat = dynamic_cast<orc::StringColumnStatistics*>(stats.get());
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
            auto* dateStat = dynamic_cast<orc::DateColumnStatistics*>(stats.get());
            py::object idx(py::int_(static_cast<int>(orc::DATE)));
            py::object from_orc =
              stripe.getReader().getConverters()[idx].attr("from_orc");
            if (dateStat->hasMinimum()) {
                result["minimum"] = from_orc(dateStat->getMinimum());
            }
            if (dateStat->hasMaximum()) {
                result["maximum"] = from_orc(dateStat->getMaximum());
            }
            return result;
        }
        case orc::TIMESTAMP: {
            auto* timeStat = dynamic_cast<orc::TimestampColumnStatistics*>(stats.get());
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
            auto* decStat = dynamic_cast<orc::DecimalColumnStatistics*>(stats.get());
            py::object idx(py::int_(static_cast<int>(orc::DECIMAL)));
            py::object from_orc =
              stripe.getReader().getConverters()[idx].attr("from_orc");
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

const orc::Type*
Column::findColumnType(const orc::Type* type)
{
    if (type->getColumnId() == columnIndex) {
        return type;
    } else {
        for (size_t i = 0; i < type->getSubtypeCount(); ++i) {
            auto* subtype = type->getSubtype(i);
            if (subtype->getColumnId() <= columnIndex &&
                subtype->getMaximumColumnId() >= columnIndex) {
                return Column::findColumnType(subtype);
            }
        }
        throw std::logic_error("type not found");
    }
}

orc::ColumnVectorBatch*
Column::selectBatch(const orc::Type& type, orc::ColumnVectorBatch* batch)
{
    if (type.getColumnId() == columnIndex) {
        return batch;
    } else {
        switch (static_cast<int64_t>(type.getKind())) {
            case orc::LIST:
                return selectBatch(
                  *type.getSubtype(0),
                  dynamic_cast<orc::ListVectorBatch*>(batch)->elements.get());
            case orc::MAP: {
                auto* tmpBatch = dynamic_cast<orc::MapVectorBatch*>(batch);
                if (type.getSubtype(0)->getColumnId() <= columnIndex &&
                    type.getSubtype(0)->getMaximumColumnId() >= columnIndex) {
                    return selectBatch(*type.getSubtype(0), tmpBatch->keys.get());
                } else {
                    return selectBatch(*type.getSubtype(1), tmpBatch->elements.get());
                }
                break;
            }
            case orc::STRUCT: {
                auto* tmpBatch = dynamic_cast<orc::StructVectorBatch*>(batch);
                for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
                    auto* subtype = type.getSubtype(i);
                    if (subtype->getColumnId() <= columnIndex &&
                        subtype->getMaximumColumnId() >= columnIndex) {
                        return selectBatch(*subtype, tmpBatch->fields[i]);
                    }
                }
            }
            case orc::UNION: {
                auto* tmpBatch = dynamic_cast<orc::UnionVectorBatch*>(batch);
                for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
                    auto* subtype = type.getSubtype(i);
                    if (subtype->getColumnId() <= columnIndex &&
                        subtype->getMaximumColumnId() >= columnIndex) {
                        return selectBatch(*subtype, tmpBatch->children[i]);
                    }
                }
            }
            default:
                throw py::value_error("unknown batch type");
        }
    }
}

py::object
Column::next()
{
    while (true) {
        if (batchItem == 0) {
            if (!rowReader->next(*batch)) {
                throw py::stop_iteration();
            }
            selectedBatch =
              this->selectBatch(rowReader->getSelectedType(), batch.get());
            converter->reset(*selectedBatch);
        }
        if (batchItem < selectedBatch->numElements) {
            py::object val = converter->toPython(batchItem);
            ++batchItem;
            ++currentRow;
            return val;
        } else {
            batchItem = 0;
        }
    }
}
