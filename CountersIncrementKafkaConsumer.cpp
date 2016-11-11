#include "counters/CountersIncrementKafkaConsumer.h"

#include <string>
#include <unordered_map>

#include "boost/endian/buffers.hpp"
#include "counters/CounterRecord.hh"
#include "folly/Format.h"
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
    CHECK(consumerHelper_->commitNextProcessOffset(offsetKey_, lastProcessedOffset_ + 1, &writeBatch));
    commitAsync();  // it's okay if commit failed, since the offset in kafkadb is the source of truth
    DLOG(INFO) << "Batch processed " << count << " messages with "
              << counts.size() / sizeof(kTimespanSuffixes) / sizeof(kTimespanSuffixes[0]) << " unique keys";
  }
}

void CountersIncrementKafkaConsumer::processOne(const RdKafka::Message& msg, void* opaque) {
  auto counts = static_cast<std::unordered_map<std::string, int64_t>*>(opaque);
  Counter record;
  consumerHelper_->decodeAvroPayload(msg.payload(), msg.len(), &record);
  std::string key(reinterpret_cast<const char*>(record.key.data()), record.key.size());
  key.append(1, 'H');  // placeholder for suffix
  for (char suffix : kTimespanSuffixes) {
    key[record.key.size()] = suffix;
    (*counts)[key] += record.by;
  }
  lastProcessedOffset_ = msg.offset();
}

constexpr char CountersIncrementKafkaConsumer::kTimespanSuffixes[];

}  // namespace counters
