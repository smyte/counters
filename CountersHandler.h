#ifndef COUNTERS_COUNTERSHANDLER_H_
#define COUNTERS_COUNTERSHANDLER_H_

#include <memory>
#include <string>
#include <vector>

#include "codec/RedisValue.h"
#include "counters/IncrbyMergeOperator.h"
#include "counters/ZeroValueCompactionFilter.h"
#include "pipeline/TransactionalRedisHandler.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

namespace counters {

class CountersHandler : public pipeline::TransactionalRedisHandler {
 public:
  CountersHandler(std::shared_ptr<pipeline::DatabaseManager> databaseManager,
                  std::shared_ptr<infra::kafka::ConsumerHelper> consumerHelper)
      : TransactionalRedisHandler(databaseManager, consumerHelper) {}

  static void optimizeColumnFamily(int defaultBlockCacheSizeMb, rocksdb::ColumnFamilyOptions* options) {
    options->compaction_filter = new ZeroValueCompactionFilter();
    options->merge_operator.reset(new IncrbyMergeOperator());
    // options->OptimizeForPointLookup(defaultBlockCacheSizeMb);
    rocksdb::BlockBasedTableOptions block_based_options;
    block_based_options.index_type = rocksdb::BlockBasedTableOptions::kBinarySearch;
    block_based_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    block_based_options.block_cache = rocksdb::NewLRUCache(static_cast<size_t>(defaultBlockCacheSizeMb * 1024 * 1024));
    options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based_options));
    options->memtable_prefix_bloom_size_ratio = 0.02;
  }

  const TransactionalCommandHandlerTable& getTransactionalCommandHandlerTable() const override {
    static const TransactionalCommandHandlerTable table(mergeWithDefaultTransactionalCommandHandlerTable({
      { "ensure", { static_cast<TransactionalCommandHandlerFunc>(&CountersHandler::ensureCommand), 2, 2 } },
      { "get", { static_cast<TransactionalCommandHandlerFunc>(&CountersHandler::getCommand), 1, 1 } },
      { "incrby", { static_cast<TransactionalCommandHandlerFunc>(&CountersHandler::incrbyCommand), 2, 2 } },
      { "set", { static_cast<TransactionalCommandHandlerFunc>(&CountersHandler::setCommand), 2, 2 } },
    }));
    return table;
  }

 private:
  using TransactionalCommandHandlerFunc = pipeline::TransactionalRedisHandler::TransactionalCommandHandlerFunc;

  codec::RedisValue ensureCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch, Context* ctx);
  codec::RedisValue getCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch, Context* ctx);
  codec::RedisValue incrbyCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch, Context* ctx);
  codec::RedisValue setCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch, Context* ctx);
};

}  // namespace counters

#endif  // COUNTERS_COUNTERSHANDLER_H_
