#include <pybind11/stl.h>

#include "PyORCStream.h"
#include "Reader.h"

py::object
ORCIterator::next()
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

py::object
ORCIterator::read(int64_t num)
{
    int64_t i = 0;
    py::list res;
    try {
        while (true) {
            res.append(this->next());
            ++i;
            if (num != -1 && i == num) {
                return res;
            }
        }
    } catch (py::stop_iteration) {
        return res;
    }
}

uint64_t
ORCIterator::seek(int64_t row, uint16_t whence)
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

Reader::Reader(py::object fileo,
               uint64_t batch_size,
               std::list<uint64_t> col_indices,
               std::list<std::string> col_names)
{
    orc::ReaderOptions readerOpts;
    batchItem = 0;
    currentRow = 0;
    firstRowOfStripe = 0;
    if (!col_indices.empty() && !col_names.empty()) {
        throw py::value_error(
          "Either col_indices or col_names can be set to select columns");
    }
    if (!col_indices.empty()) {
        try {
            rowReaderOpts = rowReaderOpts.include(col_indices);
        } catch (py::cast_error) {
            throw py::value_error("col_indices must be a sequence of integers");
        }
    }
    if (!col_names.empty()) {
        try {
            rowReaderOpts = rowReaderOpts.include(col_names);
        } catch (py::cast_error) {
            throw py::value_error("col_names must be a sequence of strings");
        }
    }
    reader = createReader(
      std::unique_ptr<orc::InputStream>(new PyORCInputStream(fileo)), readerOpts);
    try {
        batchSize = batch_size;
        rowReader = reader->createRowReader(rowReaderOpts);
        batch = rowReader->createRowBatch(batchSize);
        converter = createConverter(&rowReader->getSelectedType());
    } catch (orc::ParseError err) {
        throw py::value_error(err.what());
    }
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

Stripe
Reader::readStripe(uint64_t num)
{
    return Stripe(*this, num, reader->getStripe(num));
}

py::object
Reader::schema()
{
    const orc::Type& schema = reader->getType();
    return py::str(schema.toString());
}

Stripe::Stripe(const Reader& reader_,
               uint64_t num,
               std::unique_ptr<orc::StripeInformation> stripe)
  : reader(reader_)
{
    batchItem = 0;
    currentRow = 0;
    stripeIndex = num;
    stripeInfo = std::move(stripe);
    rowReaderOpts = reader.getRowReaderOptions();
    rowReaderOpts =
      rowReaderOpts.range(stripeInfo->getOffset(), stripeInfo->getLength());
    rowReader = reader.getORCReader().createRowReader(rowReaderOpts);
    batch = rowReader->createRowBatch(reader.getBatchSize());
    converter = createConverter(&rowReader->getSelectedType());
    firstRowOfStripe = rowReader->getRowNumber() + 1;
}

py::object
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

std::string
Stripe::writerTimezone()
{
    return stripeInfo->getWriterTimezone();
}
