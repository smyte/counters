#ifndef COUNTERS_COUNTERSINCREMENTKAFKACONSUMER_H_
#define COUNTERS_COUNTERSINCREMENTKAFKACONSUMER_H_

#include <memory>
#include <string>

#include "boost/algorithm/string/predicate.hpp"
#include "infra/kafka/Consumer.h"
#include "librdkafka/rdkafkacpp.h"

namespace counters {

class CountersIncrementKafkaConsumer : public infra::kafka::Consumer {
 public:
  static const char* name() {
    return "increment.kafka";
  }

  CountersIncrementKafkaConsumer(const std::string& brokerList, const std::string& topicStr, int partition,
                                 const std::string& groupId, const std::string& offsetKey,
                                 std::shared_ptr<infra::kafka::ConsumerHelper> consumerHelper)
      : infra::kafka::Consumer(brokerList, topicStr, partition, groupId, offsetKey, consumerHelper),
        lastProcessedOffset_(RdKafka::Topic::OFFSET_INVALID) {}

  virtual ~CountersIncrementKafkaConsumer() {}

  void stop(void) override {
    infra::kafka::Consumer::stop();
  }

  // Override kafka-related methods as needed
  // Override processBatch to allow batch-writing to rocksdb
  void processBatch(int timeoutMs) override;
  // Must override processOne to consume individual messages
  void processOne(const RdKafka::Message& msg, void* opaque) override;

 private:
  static constexpr char kTimespanSuffixes[] = { 'H', 'D', 'W', 'M', 'T' };

  int64_t lastProcessedOffset_;
};

}  // namespace counters

#endif  // COUNTERS_COUNTERSINCREMENTKAFKACONSUMER_H_
