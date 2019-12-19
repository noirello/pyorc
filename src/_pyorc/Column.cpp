#include "Column.h"

#include <cstdlib>

Column::Column(const ORCStream& stream_,
               uint64_t num,
               std::map<uint32_t, orc::BloomFilterIndex> bloomFilters)
  : columnIndex(num)
  , stream(stream_)
{
    batchItem = 0;
    currentRow = 0;
    rowReaderOpts = stream.getRowReaderOptions();
    rowReader = stream.getORCReader().createRowReader(rowReaderOpts);
    batch = rowReader->createRowBatch(stream.getBatchSize());
    const orc::Type* type = this->findColumnType(&rowReader->getSelectedType());
    converter = createConverter(type, stream.getStructKind(), stream.getConverters());
    try {
        stats = stream.createStatistics(type, columnIndex);
    } catch (std::logic_error& err) {
        throw py::index_error(err.what());
    }
    if (!bloomFilters.empty()) {
        bloomFilter = std::unique_ptr<orc::BloomFilterIndex>(
          new orc::BloomFilterIndex(bloomFilters[static_cast<uint32_t>(columnIndex)]));
    } else {
        bloomFilter = std::unique_ptr<orc::BloomFilterIndex>(nullptr);
    }
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
            py::object to_orc = stream.getConverters()[idx].attr("to_orc");
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
            py::object to_orc = stream.getConverters()[idx].attr("to_orc");
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
            py::object to_orc = stream.getConverters()[idx].attr("to_orc");
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
Column::statistics() {
    return stats;
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
        throw py::index_error("column not found");
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
