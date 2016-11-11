#ifndef COUNTERS_COUNTERSHANDLER_H_
#define COUNTERS_COUNTERSHANDLER_H_

#include <memory>
#include <string>
#include <vector>

#include "codec/RedisValue.h"
#include "counters/IncrbyMergeOperator.h"
#include "counters/ZeroValueCompactionFilter.h"
#include "pipeline/TransactionalRedisHandler.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"

namespace counters {

class CountersHandler : public pipeline::TransactionalRedisHandler {
 public:
  explicit CountersHandler(std::shared_ptr<pipeline::DatabaseManager> databaseManager)
      : TransactionalRedisHandler(databaseManager) {}

  static void optimizeColumnFamily(int defaultBlockCacheSizeMb, rocksdb::ColumnFamilyOptions* options) {
    options->compaction_filter = new ZeroValueCompactionFilter();
    options->merge_operator.reset(new IncrbyMergeOperator());
    options->OptimizeForPointLookup(defaultBlockCacheSizeMb);
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
