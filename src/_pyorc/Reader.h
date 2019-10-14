#ifndef READER_H
#define READER_H

#include <pybind11/pybind11.h>
#include "orc/OrcFile.hh"

#include "Converter.h"

namespace py = pybind11;


class ORCIterator {
protected:
    uint64_t batchItem;
    orc::RowReaderOptions rowReaderOpts;
    std::unique_ptr<orc::RowReader> rowReader;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<Converter> converter;

public:
    uint64_t currentRow;
    virtual uint64_t len() = 0;
    py::object next();
    py::object read(int64_t);
    const orc::RowReaderOptions getRowReaderOptions() const { return rowReaderOpts; };
};

class Stripe; /* Forward declaration */

class Reader : public ORCIterator {
private:
    std::unique_ptr<orc::Reader> reader;
    uint64_t batchSize;
public:
    Reader(py::object, py::object = py::int_(1024), py::object = py::none(), py::object = py::none());
    uint64_t len() override;
    uint64_t numberOfStripes();
    py::object schema();
    Stripe read_stripe(uint64_t);
    uint64_t seek(uint64_t);
    const orc::Reader& getORCReader() const { return *reader; }
    const uint64_t getBatchSize() const { return batchSize; }
};

class Stripe : public ORCIterator {
private:
    std::unique_ptr<orc::StripeInformation> stripeInfo;
public:
    Stripe(const Reader&, uint64_t, std::unique_ptr<orc::StripeInformation>);
    uint64_t len() override;
    uint64_t length();
    uint64_t offset();
    std::string writer_timezone();
};

#endif