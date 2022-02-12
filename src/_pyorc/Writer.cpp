#include "Writer.h"
#include "PyORCStream.h"

void
setTypeAttributes(orc::Type* type, py::handle schema)
{
    py::dict attributes(py::getattr(schema, "attributes"));
    for (auto attr : attributes) {
        type->setAttribute(py::cast<std::string>(attr.first),
                           py::cast<std::string>(attr.second));
    }
}

ORC_UNIQUE_PTR<orc::Type>
createType(py::handle schema)
{
    orc::TypeKind kind = orc::TypeKind(py::cast<int>(getattr(schema, "kind")));
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
        case orc::TypeKind::TIMESTAMP_INSTANT:
        case orc::TypeKind::DATE: {
            ORC_UNIQUE_PTR<orc::Type> type = orc::createPrimitiveType(kind);
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::CHAR: {
            ORC_UNIQUE_PTR<orc::Type> type = orc::createCharType(
              kind, py::cast<uint64_t>(getattr(schema, "max_length")));
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::DECIMAL: {
            uint64_t precision = py::cast<uint64_t>(getattr(schema, "precision"));
            uint64_t scale = py::cast<uint64_t>(getattr(schema, "scale"));
            ORC_UNIQUE_PTR<orc::Type> type = orc::createDecimalType(precision, scale);
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::LIST: {
            py::handle child = getattr(schema, "type");
            ORC_UNIQUE_PTR<orc::Type> type = orc::createListType(createType(child));
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::MAP: {
            py::handle key = getattr(schema, "key");
            py::handle value = getattr(schema, "value");
            ORC_UNIQUE_PTR<orc::Type> type =
              orc::createMapType(createType(key), createType(value));
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::STRUCT: {
            ORC_UNIQUE_PTR<orc::Type> type = orc::createStructType();
            py::dict fields = getattr(schema, "fields");
            for (auto item : fields) {
                type->addStructField((py::str)item.first, createType(item.second));
            }
            setTypeAttributes(type.get(), schema);
            return type;
        }
        case orc::TypeKind::UNION: {
            ORC_UNIQUE_PTR<orc::Type> type = orc::createUnionType();
            py::list cont_types = getattr(schema, "cont_types");
            for (auto child : cont_types) {
                type->addUnionChild(createType(child));
            }
            setTypeAttributes(type.get(), schema);
            return type;
        }
        default:
            throw py::type_error("Invalid TypeKind");
    }
}

Writer::Writer(py::object fileo,
               py::object schema,
               uint64_t batch_size,
               uint64_t stripe_size,
               uint64_t row_index_stride,
               int compression,
               int compression_strategy,
               uint64_t compression_block_size,
               std::set<uint64_t> bloom_filter_columns,
               double bloom_filter_fpp,
               py::object tzone,
               unsigned int struct_repr,
               py::object conv,
               double padding_tolerance,
               double dict_key_size_threshold,
               py::object null_value)
{
    currentRow = 0;
    batchItem = 0;
    ORC_UNIQUE_PTR<orc::Type> type = createType(schema);
    orc::WriterOptions options;
    py::dict converters;

    if (conv.is_none()) {
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
    options = options.setRowIndexStride(row_index_stride);
    options = options.setColumnsUseBloomFilter(bloom_filter_columns);
    options = options.setBloomFilterFPP(bloom_filter_fpp);
    options = options.setDictionaryKeySizeThreshold(dict_key_size_threshold);
    options = options.setPaddingTolerance(padding_tolerance);
    if (!tzone.is_none()) {
        std::string tzKey = py::cast<std::string>(tzone.attr("key"));
        options = options.setTimezoneName(tzKey);
    }

    outStream = std::unique_ptr<orc::OutputStream>(new PyORCOutputStream(fileo));
    writer = orc::createWriter(*type, outStream.get(), options);
    batchSize = batch_size;
    batch = writer->createRowBatch(batchSize);
    converter = createConverter(type.get(), struct_repr, converters, tzone, null_value);
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

uint64_t
Writer::writerows(py::iterable iter)
{
    uint64_t rows = 0;
    for (auto handle : iter) {
        auto obj = py::cast<py::object>(handle);
        this->write(obj);
        ++rows;
    }
    return rows;
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
Writer::addUserMetadata(py::str key, py::bytes value)
{
    writer->addUserMetadata(key, value);
}
