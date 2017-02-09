#include "counters/CountersIncrementKafkaConsumer.h"

#include <string>
#include <unordered_map>

#include "boost/endian/buffers.hpp"
#include "counters/CounterRecord.hh"
#include "counters/CountersTimespans.h"
#include "folly/Format.h"
#include "infra/AvroHelper.h"
#include "rocksdb/write_batch.h"

namespace counters {

void CountersIncrementKafkaConsumer::processBatch(int timeoutMs) {
  std::unordered_map<std::string, int64_t> counts;
  int64_t prevOffset = lastProcessedOffset_;
  size_t count = consumeBatch(timeoutMs, &counts);
  if (lastProcessedOffset_ > prevOffset) {
    rocksdb::WriteBatch writeBatch;
    for (const auto& entry : counts) {
      boost::endian::big_int64_buf_t value(entry.second);
      writeBatch.Merge(entry.first, rocksdb::Slice(value.data(), sizeof(int64_t)));
    }
    CHECK(consumerHelper()->commitNextProcessOffset(offsetKey(), lastProcessedOffset_ + 1, &writeBatch));
    commitAsync();  // it's okay if commit failed, since the offset in kafkadb is the source of truth
    DLOG(INFO) << "Batch processed " << count << " messages with " << counts.size() << " keys";
  }
}

void CountersIncrementKafkaConsumer::processOne(const RdKafka::Message& msg, void* opaque) {
  auto counts = static_cast<std::unordered_map<std::string, int64_t>*>(opaque);
  Counter record;
  infra::AvroHelper::decode(msg.payload(), msg.len(), &record);
  std::string key(reinterpret_cast<const char*>(record.key.data()), record.key.size());
  int64_t timespanFlags = record.flags || CountersTimespans::kDefaultTimespanFlags;
  for (const auto& entry : CountersTimespans::kTimespanMap) {
    const auto& timespan = entry.second;
    if (timespanFlags & timespan.mask) {
      (*counts)[key + timespan.keySuffix] += record.by;
    }
  }
  lastProcessedOffset_ = msg.offset();
}

}  // namespace counters
