#include "PyORCStream.h"
#include "Writer.h"

ORC_UNIQUE_PTR<orc::Type> createType(py::handle schema) {
    orc::TypeKind kind = orc::TypeKind((int)py::int_(getattr(schema, "kind")));
    switch (kind) {
        case orc::TypeKind::BOOLEAN:
        case orc::TypeKind::BYTE:
        case orc::TypeKind::SHORT:
        case orc::TypeKind::INT:
        case orc::TypeKind::LONG:
        case orc::TypeKind::FLOAT:
        case orc::TypeKind::DOUBLE:
        case orc::TypeKind::STRING:
        case orc::TypeKind::BINARY:
        case orc::TypeKind::TIMESTAMP:
        case orc::TypeKind::DATE:
            return orc::createPrimitiveType(kind);
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::CHAR:
            return orc::createCharType(kind, (uint64_t)py::int_(getattr(schema, "max_length")));
        case orc::TypeKind::DECIMAL: {
            uint64_t precision = (uint64_t)py::int_(getattr(schema, "precision"));
            uint64_t scale = (uint64_t)py::int_(getattr(schema, "scale"));
            return orc::createDecimalType(precision, scale);
        }
        case orc::TypeKind::LIST: {
            py::handle child = getattr(schema, "type");
            return orc::createListType(createType(child));
        }
        case orc::TypeKind::MAP: {
            py::handle key = getattr(schema, "key");
            py::handle value = getattr(schema, "value");
            return orc::createMapType(createType(key), createType(value));
        }
        case orc::TypeKind::STRUCT: {
            ORC_UNIQUE_PTR<orc::Type> ty = orc::createStructType();
            py::dict fields = getattr(schema, "fields");
            for (auto item : fields) {
                ty->addStructField((py::str)item.first, createType(item.second));
            }
            return ty;
        }
        case orc::TypeKind::UNION: {
            ORC_UNIQUE_PTR<orc::Type> ty = orc::createUnionType();
            py::list cont_types = getattr(schema, "cont_types");
            for (auto child : cont_types) {
                ty->addUnionChild(createType(child));
            }
            return ty;
        }
        default:
            throw py::type_error("Invalid TypeKind");
    }
}

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
    ORC_UNIQUE_PTR<orc::Type> type = createType(schema);
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
