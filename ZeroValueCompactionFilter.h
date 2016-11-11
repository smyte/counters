#ifndef COUNTERS_ZEROVALUECOMPACTIONFILTER_H_
#define COUNTERS_ZEROVALUECOMPACTIONFILTER_H_

#include <string>

#include "boost/endian/buffers.hpp"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace counters {

class ZeroValueCompactionFilter : public rocksdb::CompactionFilter {
 public:
  virtual ~ZeroValueCompactionFilter() {}

  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override {
    *value_changed = false;
    CHECK_EQ(existing_value.size(), sizeof(int64_t));
    int64_t intValue = boost::endian::detail::load_big_endian<int64_t, sizeof(int64_t)>(existing_value.data());
    // delete the key when value is zero
    return intValue == 0;
  }

  const char* Name() const override {
    return "CountersZeroValueCompactionFilter";
  }
};

}  // namespace counters

#endif  // COUNTERS_ZEROVALUECOMPACTIONFILTER_H_
