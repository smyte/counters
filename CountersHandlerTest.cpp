#include <memory>
#include <string>
#include <vector>

#include "codec/RedisValue.h"
#include "counters/CountersHandler.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "stesting/TestWithRocksDb.h"

namespace counters {

class CountersHandlerTest : public stesting::TestWithRocksDb {
 protected:
  CountersHandlerTest()
    : stesting::TestWithRocksDb({}, {{"default", CountersHandler::optimizeColumnFamily}}) {}
};

class MockCountersHandler : public CountersHandler {
 public:
  explicit MockCountersHandler(std::shared_ptr<pipeline::DatabaseManager> databaseManager)
      : CountersHandler(databaseManager) {}

  MOCK_METHOD2(write, folly::Future<folly::Unit>(Context*, codec::RedisValue));
};

TEST_F(CountersHandlerTest, EnsureCommand) {
  MockCountersHandler handler(databaseManager());

  // seed values
  boost::endian::big_int64_buf_t value1(10);
  db()->Put(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value1.data(), sizeof(int64_t)));

  // same value
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kSimpleString, "OK")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("ensure", { "ensure", "key1", "10" }, nullptr));

  // different values
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kError, "ENSURE value different")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("ensure", { "ensure", "key1", "5" }, nullptr));

  // key not found
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kError, "ENSURE key not found")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("ensure", { "ensure", "key2", "5" }, nullptr));

  // value not a valid integer
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kError,
                                                        "Value is not an integer or out of range")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("ensure", { "ensure", "key2", "a" }, nullptr));
}

TEST_F(CountersHandlerTest, GetCommand) {
  MockCountersHandler handler(databaseManager());

  // seed values
  boost::endian::big_int64_buf_t value1(10);
  db()->Put(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value1.data(), sizeof(int64_t)));

  // key exists
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(10)))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("get", { "get", "key1" }, nullptr));

  // key does not exist
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue::nullString()))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("get", { "get", "key2" }, nullptr));
}

TEST_F(CountersHandlerTest, IncrbyCommand) {
  MockCountersHandler handler(databaseManager());

  // seed values
  boost::endian::big_int64_buf_t value1(10);
  db()->Put(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value1.data(), sizeof(int64_t)));

  // value not a valid integer
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kError,
                                                        "Value is not an integer or out of range")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("incrby", { "incrby", "key1", "a" }, nullptr));

  // key exists
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(15)))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("incrby", { "incrby", "key1", "5" }, nullptr));

  // key does not exist
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(-5)))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("incrby", { "incrby", "key2", "-5" }, nullptr));
}

TEST_F(CountersHandlerTest, SetCommand) {
  MockCountersHandler handler(databaseManager());

  // value not a valid integer
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kError,
                                                        "Value is not an integer or out of range")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("set", { "set", "key1", "a" }, nullptr));

  // valid integer
  EXPECT_CALL(handler, write(nullptr, codec::RedisValue(codec::RedisValue::Type::kSimpleString, "OK")))
      .Times(1);
  EXPECT_TRUE(handler.handleCommand("set", { "set", "key1", "10" }, nullptr));
  std::string newValue1;
  rocksdb::Status s1 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue1);
  EXPECT_TRUE(s1.ok());
  int64_t intNewValue1 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue1.data());
  EXPECT_EQ(10, intNewValue1);
}

TEST_F(CountersHandlerTest, ZeroValueCompactionFilter) {
  // no change after compaction for non-zero values
  boost::endian::big_int64_buf_t value1(10);
  db()->Put(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value1.data(), sizeof(int64_t)));
  db()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  std::string newValue1;
  rocksdb::Status s1 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue1);
  EXPECT_TRUE(s1.ok());
  int64_t intNewValue1 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue1.data());
  EXPECT_EQ(10, intNewValue1);

  // compaction deletes zero values
  boost::endian::big_int64_buf_t value2(0);
  db()->Put(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value2.data(), sizeof(int64_t)));
  std::string newValue2;
  rocksdb::Status s2 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue2);
  EXPECT_TRUE(s2.ok());
  int64_t intNewValue2 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue2.data());
  EXPECT_EQ(0, intNewValue2);
  db()->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  s2 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue2);
  EXPECT_TRUE(s2.IsNotFound());
}

TEST_F(CountersHandlerTest, IncrbyMergeOperator) {
  // no existing value
  boost::endian::big_int64_buf_t value1(10);
  db()->Merge(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value1.data(), sizeof(int64_t)));
  std::string newValue1;
  rocksdb::Status s1 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue1);
  EXPECT_TRUE(s1.ok());
  int64_t intNewValue1 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue1.data());
  EXPECT_EQ(10, intNewValue1);

  // with existing values
  boost::endian::big_int64_buf_t value2(5);
  db()->Merge(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value2.data(), sizeof(int64_t)));
  std::string newValue2;
  rocksdb::Status s2 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue2);
  EXPECT_TRUE(s2.ok());
  int64_t intNewValue2 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue2.data());
  EXPECT_EQ(15, intNewValue2);

  boost::endian::big_int64_buf_t value3(-16);
  db()->Merge(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value3.data(), sizeof(int64_t)));
  std::string newValue3;
  rocksdb::Status s3 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue3);
  EXPECT_TRUE(s3.ok());
  int64_t intNewValue3 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue3.data());
  EXPECT_EQ(-1, intNewValue3);

  boost::endian::big_int64_buf_t value4(1);
  db()->Merge(rocksdb::WriteOptions(), "key1", rocksdb::Slice(value4.data(), sizeof(int64_t)));
  std::string newValue4;
  rocksdb::Status s4 = db()->Get(rocksdb::ReadOptions(), "key1", &newValue4);
  EXPECT_TRUE(s4.ok());
  int64_t intNewValue4 = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(newValue4.data());
  EXPECT_EQ(0, intNewValue4);
}

}  // namespace counters
