#ifndef READER_H
#define READER_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "orc/OrcFile.hh"

#include "Converter.h"

namespace py = pybind11;

class ORCIterator
{
  protected:
    uint64_t batchItem;
    orc::RowReaderOptions rowReaderOpts;
    std::unique_ptr<orc::RowReader> rowReader;
    std::unique_ptr<orc::ColumnVectorBatch> batch;
    std::unique_ptr<Converter> converter;

  public:
    uint64_t currentRow;
    uint64_t firstRowOfStripe;
    virtual uint64_t len() const = 0;
    py::object next();
    py::object read(int64_t);
    uint64_t seek(uint64_t);
    const orc::RowReaderOptions getRowReaderOptions() const { return rowReaderOpts; };
};

class Stripe; /* Forward declaration */

class Reader : public ORCIterator
{
  private:
    std::unique_ptr<orc::Reader> reader;
    uint64_t batchSize;

  public:
    Reader(py::object,
           uint64_t = 1024,
           std::list<uint64_t> = {},
           std::list<std::string> = {});
    uint64_t len() const override;
    uint64_t numberOfStripes() const;
    py::object schema();
    Stripe readStripe(uint64_t);

    const orc::Reader& getORCReader() const { return *reader; }
    const uint64_t getBatchSize() const { return batchSize; }
};

class Stripe : public ORCIterator
{
  private:
    std::unique_ptr<orc::StripeInformation> stripeInfo;

  public:
    Stripe(const Reader&, uint64_t, std::unique_ptr<orc::StripeInformation>);
    uint64_t len() const override;
    uint64_t length() const;
    uint64_t offset() const;
    std::string writerTimezone();
};

#endif