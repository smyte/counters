#include "counters/CountersHandler.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "boost/endian/buffers.hpp"
#include "folly/Conv.h"
#include "glog/logging.h"
#include "codec/RedisValue.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace counters {

codec::RedisValue CountersHandler::ensureCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch,
                                                 Context* ctx) {
  rocksdb::Slice key = rocksdb::Slice(cmd[1]);
  int64_t desiredValue = 0;
  try {
    desiredValue = folly::to<int64_t>(cmd[2]);
  } catch (std::range_error&) {
    return errorInvalidInteger();
  }

  std::string value;
  // TODO(yunjing): support read-your-own-write by search the write batch first, when such guaranteed is needed
  rocksdb::Status status = db()->Get(rocksdb::ReadOptions(), key, &value);

  if (status.ok()) {
    CHECK_EQ(value.size(), sizeof(int64_t));
    if (desiredValue == boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(value.data())) {
      return simpleStringOk();
    }
    return { codec::RedisValue::Type::kError, "ENSURE value different" };
  } else if (status.IsNotFound()) {
    return { codec::RedisValue::Type::kError, "ENSURE key not found" };
  }

  return errorResp(folly::sformat("RocksDB error: {}", status.ToString()));
}

codec::RedisValue CountersHandler::getCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch,
                                              Context* ctx) {
  rocksdb::Slice key = rocksdb::Slice(cmd[1]);

  std::string value;
  // TODO(yunjing): support read-your-own-write by search the write batch first, when such guaranteed is needed
  rocksdb::Status status = db()->Get(rocksdb::ReadOptions(), key, &value);

  if (status.ok()) {
    CHECK_EQ(value.size(), sizeof(int64_t));
    return codec::RedisValue(boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(value.data()));
  } else if (status.IsNotFound()) {
    return codec::RedisValue::nullString();
  }

  return errorResp(folly::sformat("RocksDB error: {}", status.ToString()));
}

codec::RedisValue CountersHandler::incrbyCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch,
                                                 Context* ctx) {
  rocksdb::Slice key = rocksdb::Slice(cmd[1]);
  int64_t delta = 0;
  try {
    delta = folly::to<int64_t>(cmd[2]);
  } catch (std::range_error&) {
    return errorInvalidInteger();
  }

  boost::endian::big_int64_buf_t value(delta);
  // using merge to ensure atomicity with respect to multiple concurrent incrby operations
  writeBatch->Merge(key, rocksdb::Slice(value.data(), sizeof(int64_t)));
  std::string prevValue;
  // reading existing from database is still subject to race condition when there is a concurrent write,
  // but the returned value is guaranteed to be one of many legit values under certain interleaving of writes
  // importantly, the side-effect of the race condition is eliminated by using merge.
  rocksdb::Status status = db()->Get(rocksdb::ReadOptions(), key, &prevValue);

  if (status.ok()) {
    CHECK_EQ(prevValue.size(), sizeof(int64_t));
    int64_t prevInt = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(prevValue.data());
    return codec::RedisValue(prevInt + delta);
  } else if (status.IsNotFound()) {
    return codec::RedisValue(delta);
  }

  return errorResp(folly::sformat("RocksDB error: {}", status.ToString()));
}

codec::RedisValue CountersHandler::setCommand(const std::vector<std::string>& cmd, rocksdb::WriteBatch* writeBatch,
                                              Context* ctx) {
  rocksdb::Slice key = rocksdb::Slice(cmd[1]);
  try {
    boost::endian::big_int64_buf_t value(folly::to<int64_t>(cmd[2]));
    writeBatch->Put(key, rocksdb::Slice(value.data(), sizeof(int64_t)));
  } catch (std::range_error&) {
    return errorInvalidInteger();
  }

  return simpleStringOk();
}

}  // namespace counters
