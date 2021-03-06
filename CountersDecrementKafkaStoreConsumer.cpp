#include "counters/CountersDecrementKafkaStoreConsumer.h"

#include <algorithm>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <utility>

#include "boost/endian/buffers.hpp"
#include "counters/CounterRecord.hh"
#include "folly/Format.h"
#include "glog/logging.h"
#include "infra/AvroHelper.h"

namespace counters {

using infra::kafka::store::KafkaStoreMessage;

void CountersDecrementKafkaStoreConsumer::processBatch(int timeoutMs) {
  ProcessingBuf buf = {};
  int64_t count = consumeBatch(timeoutMs, &buf);
  LOG(INFO) << "Read " << count << " messages in `" << mode_ << "` mode";
  commitCounts(buf);

  // processed delayed messages
  std::map<int64_t, infra::kafka::store::KafkaStoreMessage> delayedMsgs = std::move(buf.msgBuf);
  while (run() && !delayedMsgs.empty()) {
    // delay until the first message is due
    if (!delay(timeDelayMs_, delayedMsgs.begin()->second.timestamp)) {
      // Break early due to failed delay, e.g., the program is being terminated
      break;
    }
    ProcessingBuf delayedBuf = {};
    for (const auto& entry : delayedMsgs) {
      processOne(entry.first, entry.second, &delayedBuf);
    }
    commitCounts(delayedBuf);
    delayedMsgs = std::move(delayedBuf.msgBuf);
  }
}

void CountersDecrementKafkaStoreConsumer::processOne(int64_t offset, const infra::kafka::store::KafkaStoreMessage& msg,
                                                     void* opaque) {
  auto buf = static_cast<ProcessingBuf*>(opaque);
  if (!buf->msgBuf.empty()) {
    // Assume that timestamps from kafka store messages are monotonically increasing
    // so once one message was buffered for delayed processing, all subsequent messages should follow
    buf->msgBuf.insert(std::make_pair(offset, msg));
    return;
  }
  if (msg.value.is_null()) {
    buf->nextProcessOffset = offset + 1;
    LOG(ERROR) << "Message value at offset " << offset << " is null";
    return;
  }

  auto valBytes = msg.value.get_bytes();
  Counter record;
  infra::AvroHelper::decode(valBytes.data(), valBytes.size(), &record);
  if (nowMs() - msg.timestamp >= timeDelayMs_) {
    // this message is overdue, apply the count
    std::string key(reinterpret_cast<const char*>(record.key.data()), record.key.size());
    int64_t timespanFlags = record.flags ? record.flags : CountersTimespans::kDefaultTimespanFlags;
    if (timespanFlags & timespanMask_) {
      key.append(keySuffix_);
      buf->counts[key] -= record.by;
    }
    buf->nextProcessOffset = offset + 1;
  } else {
    // save the messaged for delayed processing
    buf->msgBuf.insert(std::make_pair(offset, msg));
  }
}

void CountersDecrementKafkaStoreConsumer::commitCounts(const CountersDecrementKafkaStoreConsumer::ProcessingBuf& buf) {
  if (buf.counts.empty() && buf.msgBuf.empty()) {
    // The entire batch is empty
    return;
  }

  int64_t nextOffset = buf.nextProcessOffset >= 0 ? buf.nextProcessOffset : buf.msgBuf.begin()->first;
  rocksdb::WriteBatch writeBatch;
  for (const auto& entry : buf.counts) {
    boost::endian::big_int64_buf_t value(entry.second);
    writeBatch.Merge(entry.first, rocksdb::Slice(value.data(), sizeof(int64_t)));
  }
  int64_t fileOffset = buf.nextProcessOffset < nextFileOffset() ? currentFileOffset() : nextFileOffset();
  CHECK(consumerHelper()->commitNextProcessKafkaAndFileOffsets(offsetKey(), nextOffset, fileOffset, &writeBatch));
  // Also commit to kafka brokers only for metrics and reporting, so failure is okay
  if (!commitAsync()) {
    LOG(WARNING) << "Committing offset to kafka brokers failed";
  }
}

bool CountersDecrementKafkaStoreConsumer::delay(int64_t delayMs, int64_t timeMs) {
  // Add an extra margin to delay time so that more keys are grouped for committing
  int64_t waitUntilMs = timeMs + delayMs + kDelayMarginMs;
  int64_t sleepTimeMs = waitUntilMs - nowMs();

  if (sleepTimeMs <= 0) return true;
  DLOG(INFO) << "Sleeping for " << sleepTimeMs << "ms for delay in `" << mode_ << "` mode";
  while (sleepTimeMs > 0) {
    if (!run()) return false;
    std::this_thread::sleep_for(std::min(std::chrono::milliseconds(1000), std::chrono::milliseconds(sleepTimeMs)));
    sleepTimeMs = waitUntilMs - nowMs();
  }
  return true;
}

}  // namespace counters
