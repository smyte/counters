#include "counters/CountersTimespans.h"

#include <chrono>

namespace counters {

using milliseconds = std::chrono::milliseconds;
using hours = std::chrono::hours;

const std::unordered_map<std::string, CountersTimespans::Timespan> CountersTimespans::kTimespanMap =
    []() -> std::unordered_map<std::string, CountersTimespans::Timespan> {
  return {
      {"hour", {std::chrono::duration_cast<milliseconds>(hours(1)).count(), "H", 1L}},
      {"day", {std::chrono::duration_cast<milliseconds>(hours(24)).count(), "D", 2L}},
      {"week", {std::chrono::duration_cast<milliseconds>(hours(24 * 7)).count(), "W", 4L}},
      {"month", {std::chrono::duration_cast<milliseconds>(hours(24 * 30)).count(), "M", 8L}},
      {"total", {-1L, "T", 16L}},
      {"2days", {std::chrono::duration_cast<milliseconds>(hours(24 * 2)).count(), "D2", 32L}},
      {"2weeks", {std::chrono::duration_cast<milliseconds>(hours(24 * 14)).count(), "W2", 64L}},
      {"8days", {std::chrono::duration_cast<milliseconds>(hours(24 * 8)).count(), "D8", 128L}},
      {"6months", {std::chrono::duration_cast<milliseconds>(hours(24 * 180)).count(), "M6", 256L}},
  };
}();

const int64_t CountersTimespans::kDefaultTimespanFlags =
    CountersTimespans::kTimespanMap.at("hour").mask | CountersTimespans::kTimespanMap.at("day").mask |
    CountersTimespans::kTimespanMap.at("week").mask | CountersTimespans::kTimespanMap.at("month").mask;

}  // namespace counters
