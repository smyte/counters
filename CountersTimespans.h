#ifndef COUNTERS_COUNTERSTIMESPANS_H_
#define COUNTERS_COUNTERSTIMESPANS_H_

#include <string>
#include <unordered_map>
#include <utility>

namespace counters {

class CountersTimespans {
 public:
  struct Timespan {
    int64_t timeDelayMs;
    std::string keySuffix;
    int64_t mask;
    Timespan() : timeDelayMs(-1), keySuffix(), mask(0) {}

    Timespan(int64_t _timeDelayMs, std::string _keySuffix, int64_t _mask)
        : timeDelayMs(_timeDelayMs), keySuffix(std::move(_keySuffix)), mask(_mask) {}
  };

  // Mode -> Timespan
  static const std::unordered_map<std::string, Timespan> kTimespanMap;
  static const int64_t kDefaultTimespanFlags;
};

}  // namespace counters

#endif  // COUNTERS_COUNTERSTIMESPANS_H_
