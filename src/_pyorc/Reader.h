#ifndef READER_H
#define READER_H

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "orc/OrcFile.hh"

#include "Converter.h"
#include "TypeDescription.h"

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
    virtual py::object next();
    py::object read(int64_t = -1);
    const orc::RowReaderOptions getRowReaderOptions() const { return rowReaderOpts; };
};

class ORCStream : public ORCIterator
{
  private:
    py::object convertTimestampMillis(int64_t) const;

  protected:
    py::dict converters;
    uint64_t batchSize;
    unsigned int structKind;
    py::object buildStatistics(const orc::Type*, const orc::ColumnStatistics*) const;

  public:
    virtual uint64_t len() const = 0;
    uint64_t seek(int64_t, uint16_t = 0);
    const py::dict getConverters() const { return converters; }
    const uint64_t getBatchSize() const { return batchSize; }
    const unsigned int getStructKind() const { return structKind; }
    virtual const orc::Reader& getORCReader() const = 0;
    virtual py::tuple createStatistics(const orc::Type*, uint64_t) const = 0;
};

/* Forward declarations */
class Stripe;
class Column;

class Reader : public ORCStream
{
  private:
    std::unique_ptr<orc::Reader> reader;
    std::unique_ptr<TypeDescription> typeDesc;

  public:
    Reader(py::object,
           uint64_t = 1024,
           std::list<uint64_t> = {},
           std::list<std::string> = {},
           unsigned int = 0,
           py::object = py::none());
    uint64_t len() const override;
    uint64_t numberOfStripes() const;
    TypeDescription& schema();
    Stripe readStripe(uint64_t);
    Column getItem(uint64_t);
    py::tuple createStatistics(const orc::Type*, uint64_t) const override;

    const orc::Reader& getORCReader() const override { return *reader; }
};

class Stripe : public ORCStream
{
  private:
    uint64_t stripeIndex;
    std::unique_ptr<orc::StripeInformation> stripeInfo;
    const Reader& reader;

  public:
    Stripe(const Reader&, uint64_t, std::unique_ptr<orc::StripeInformation>);
    py::object bloomFilterColumns();
    Column getItem(uint64_t);
    uint64_t len() const override;
    uint64_t length() const;
    uint64_t offset() const;
    std::string writerTimezone();
    py::tuple createStatistics(const orc::Type*, uint64_t) const override;

    const Reader& getReader() const { return reader; }
    const orc::Reader& getORCReader() const override { return reader.getORCReader(); }
};

#endif