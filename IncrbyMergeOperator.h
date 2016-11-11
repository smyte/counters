#ifndef COUNTERS_INCRBYMERGEOPERATOR_H_
#define COUNTERS_INCRBYMERGEOPERATOR_H_

#include <string>

#include "boost/endian/buffers.hpp"
#include "glog/logging.h"
#include "rocksdb/merge_operator.h"

namespace counters {

using boost::endian::detail::load_big_endian;
class IncrbyMergeOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual ~IncrbyMergeOperator() {}

  bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const rocksdb::Slice& value,
             std::string* new_value, rocksdb::Logger* logger) const override {
    int64_t intExistingValue = 0;
    if (existing_value) {
      CHECK_EQ(existing_value->size(), sizeof(int64_t));
      intExistingValue = load_big_endian<int64_t, sizeof(int64_t)>(existing_value->data());
    }

    CHECK_EQ(value.size(), sizeof(int64_t));
    int64_t intValue = load_big_endian<int64_t, sizeof(int64_t)>(value.data());

    boost::endian::big_int64_buf_t newValueBuf(intExistingValue + intValue);
    new_value->assign(newValueBuf.data(), sizeof(int64_t));

    return true;
  }

  const char* Name() const override {
    return "CountersIncrbyMergeOperator";
  }
};

}  // namespace counters

#endif  // COUNTERS_INCRBYMERGEOPERATOR_H_
