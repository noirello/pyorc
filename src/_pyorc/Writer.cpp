#include "PyORCStream.h"
#include "Writer.h"

Writer::Writer(py::object fileo,
               py::object schema,
               uint64_t batch_size,
               uint64_t stripe_size,
               int compression,
               int compression_strategy,
               uint64_t compression_block_size,
               std::set<uint64_t> bloom_filter_columns,
               double bloom_filter_fpp,
               unsigned int struct_repr,
               py::object conv)
{
    currentRow = 0;
    batchItem = 0;
    std::unique_ptr<orc::Type> type =
      orc::Type::buildTypeFromString(py::cast<std::string>(py::str(schema)));
    orc::WriterOptions options;
    py::dict converters;

    if (conv.is(py::none())) {
        py::dict defaultConv =
          py::module::import("pyorc.converters").attr("DEFAULT_CONVERTERS");
        converters = py::dict(defaultConv);
    } else {
        converters = conv;
    }

    options = options.setCompression(static_cast<orc::CompressionKind>(compression));
    options = options.setCompressionStrategy(
      static_cast<orc::CompressionStrategy>(compression_strategy));
    options = options.setCompressionBlockSize(compression_block_size);
    options = options.setStripeSize(stripe_size);
    options = options.setColumnsUseBloomFilter(bloom_filter_columns);
    options = options.setBloomFilterFPP(bloom_filter_fpp);

    outStream = std::unique_ptr<orc::OutputStream>(new PyORCOutputStream(fileo));
    writer = orc::createWriter(*type, outStream.get(), options);
    batchSize = batch_size;
    batch = writer->createRowBatch(batchSize);
    converter = createConverter(type.get(), struct_repr, converters);
}

void
Writer::write(py::object row)
{
    converter->write(batch.get(), batchItem, row);
    currentRow++;
    batchItem++;

    if (batchItem == batchSize) {
        writer->add(*batch);
        converter->clear();
        batchItem = 0;
    }
}

void
Writer::close()
{
    if (batchItem != 0) {
        writer->add(*batch);
        converter->clear();
        batchItem = 0;
    }
    writer->close();
}

void
Writer::addMetadata(py::str key, py::bytes value)
{
    writer->addUserMetadata(py::cast<std::string>(key), py::str(value));
}