#include "Writer.h"
#include "PyORCStream.h"

Writer::Writer(py::object fileo, std::string schema_str, uint64_t batch_size,
               uint64_t stripe_size, int compression, int compression_strategy) {
    currentRow = 0;
    batchItem = 0;
    std::unique_ptr<orc::Type> schema(orc::Type::buildTypeFromString(schema_str));
    orc::WriterOptions options;

    options = options.setCompression(static_cast<orc::CompressionKind>(compression));
    options = options.setCompressionStrategy(static_cast<orc::CompressionStrategy>(compression_strategy));
    options = options.setStripeSize(stripe_size);

    outStream = std::unique_ptr<orc::OutputStream>(new PyORCOutputStream(fileo));
    writer = createWriter(*schema, outStream.get(), options);
    batchSize = batch_size;
    batch = writer->createRowBatch(batchSize);
    converter = createConverter(schema.get());
}

void Writer::write(py::object row) {
    converter->write(batch.get(), batchItem, row);
    currentRow++; batchItem++;

    if (batchItem == batchSize) {
        writer->add(*batch);
        converter->clear();
        batchItem = 0;
    }
}

void Writer::close() {
    if (batchItem != 0) {
        writer->add(*batch);
        converter->clear();
        batchItem = 0;
    }
    writer->close();
}