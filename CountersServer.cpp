#include <memory>
#include <string>

#include "counters/CountersDecrementKafkaStoreConsumer.h"
#include "counters/CountersHandler.h"
#include "counters/CountersIncrementKafkaConsumer.h"
#include "pipeline/RedisPipelineBootstrap.h"
#include "platform/gcloud/GoogleCloudStorage.h"

namespace counters {

static pipeline::RedisPipelineBootstrap::Config config{
  redisHandlerFactory : [](const pipeline::RedisPipelineBootstrap::OptionalComponents& optionalComponents)
      -> std::shared_ptr<pipeline::RedisHandler> {
    return std::make_shared<CountersHandler>(optionalComponents.databaseManager);
  },

  kafkaConsumerFactoryMap :
      {{
           CountersIncrementKafkaConsumer::name(),
           [](const std::string& brokerList, const pipeline::KafkaConsumerConfig& consumerConfig,
              const std::string& offsetKey, std::shared_ptr<pipeline::DatabaseManager> databaseManager,
              std::shared_ptr<infra::kafka::ConsumerHelper> consumerHelper,
              std::shared_ptr<infra::ScheduledTaskQueue> scheduledTaskQueue)
               -> std::shared_ptr<infra::kafka::AbstractConsumer> {
             return std::make_shared<CountersIncrementKafkaConsumer>(brokerList, consumerConfig.topic,
                                                                     consumerConfig.partition, consumerConfig.groupId,
                                                                     offsetKey, consumerHelper);
           },
       },
       {
           CountersDecrementKafkaStoreConsumer::name(),
           [](const std::string& brokerList, const pipeline::KafkaConsumerConfig& consumerConfig,
              const std::string& offsetKey, std::shared_ptr<pipeline::DatabaseManager> databaseManager,
              std::shared_ptr<infra::kafka::ConsumerHelper> consumerHelper,
              std::shared_ptr<infra::ScheduledTaskQueue> scheduledTaskQueue)
               -> std::shared_ptr<infra::kafka::AbstractConsumer> {
             return std::make_shared<CountersDecrementKafkaStoreConsumer>(
                 brokerList, consumerConfig.objectStoreBucketName, consumerConfig.objectStoreObjectNamePrefix,
                 consumerConfig.topic, consumerConfig.partition, consumerConfig.groupId, offsetKey,
                 consumerConfig.offsetKeySuffix, consumerHelper,
                 std::make_shared<platform::gcloud::GoogleCloudStorage>());
           },
       }},

  databaseManagerFactory : nullptr,

  scheduledTaskQueueFactory : nullptr,

  rocksDbConfiguratorMap : {
      {
          pipeline::DatabaseManager::defaultColumnFamilyName(), CountersHandler::optimizeColumnFamily,
      },
  },

  singletonRedisHandler : false,  // in order to support transactions
};

static auto redisPipelineBootstrap = pipeline::RedisPipelineBootstrap::create(config);

}  // namespace counters
