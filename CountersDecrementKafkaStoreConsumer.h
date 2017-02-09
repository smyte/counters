#ifndef COUNTERS_COUNTERSDECREMENTKAFKASTORECONSUMER_H_
#define COUNTERS_COUNTERSDECREMENTKAFKASTORECONSUMER_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/algorithm/string/predicate.hpp"
#include "counters/CountersTimespans.h"
#include "infra/kafka/store/Consumer.h"
#include "infra/kafka/store/KafkaStoreMessageRecord.hh"

namespace counters {

class CountersDecrementKafkaStoreConsumer : public infra::kafka::store::Consumer {
 public:
  static const char* name() {
    return "decrement.kafka-store";
  }

  CountersDecrementKafkaStoreConsumer(const std::string& brokerList, const std::string& objectStoreBucketName,
                                      const std::string& objectStoreObjectNamePrefix, const std::string& topic,
                                      int partition, const std::string& groupId, const std::string& offsetKey,
                                      const std::string& mode,
                                      std::shared_ptr<infra::kafka::ConsumerHelper> consumerHelper,
                                      std::shared_ptr<platform::gcloud::GoogleCloudStorage> gcs)
      : infra::kafka::store::Consumer(brokerList, objectStoreBucketName, objectStoreObjectNamePrefix, topic, partition,
                                      groupId, offsetKey, consumerHelper, gcs),
        mode_(mode) {
    const auto it = CountersTimespans::kTimespanMap.find(mode);
    CHECK(it != CountersTimespans::kTimespanMap.end()) << "Unknown mode: " << mode;
    timeDelayMs_ = it->second.timeDelayMs;
    keySuffix_ = it->second.keySuffix;
    timespanMask_ = it->second.mask;
  }

  // Process a batch of messages and wait for the right time to decrement
  void processBatch(int timeoutMs) override;

  // Process one message from kafka store
  void processOne(int64_t offset, const infra::kafka::store::KafkaStoreMessage& msg, void* opaque) override;

 private:
  struct ProcessingBuf {
    // counts from processed messages
    std::unordered_map<std::string, int64_t> counts;
    // buffer for messages to be processed after a delay, keyed by kafka offset
    std::map<int64_t, infra::kafka::store::KafkaStoreMessage> msgBuf;
    int64_t nextProcessOffset;
  };

  // Allow a margin of error in time delay in order to group more keys in a single transaction
  static constexpr int64_t kDelayMarginMs = 1000;

  // Commit counts that are overdue
  void commitCounts(const ProcessingBuf& buf);

  // Delay timeMs for up to delayMs. Return true when delay was incurred successfully and false if interrupted.
  bool delay(int64_t delayMs, int64_t timeMs);

  const std::string mode_;
  int64_t timeDelayMs_;
  std::string keySuffix_;
  int64_t timespanMask_;
};

}  // namespace counters

#endif  // COUNTERS_COUNTERSDECREMENTKAFKASTORECONSUMER_H_
