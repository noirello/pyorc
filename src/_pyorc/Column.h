#include "Reader.h"

class Column : public ORCIterator
{
  private:
    uint64_t columnIndex;
    int64_t typeKind;
    py::tuple stats;
    std::unique_ptr<orc::BloomFilterIndex> bloomFilter;
    const ORCStream& stream;
    orc::ColumnVectorBatch* selectedBatch;

    bool testBloomFilter(py::object);
    const orc::Type* findColumnType(const orc::Type*);
    orc::ColumnVectorBatch* selectBatch(const orc::Type&, orc::ColumnVectorBatch*);
    py::object convertTimestampMillis(int64_t);
    void jumpToPosition(int64_t, uint64_t);

  public:
    Column(const ORCStream&, uint64_t, std::map<uint32_t, orc::BloomFilterIndex>);
    bool contains(py::object);
    py::object statistics();
    py::object next() override;
};