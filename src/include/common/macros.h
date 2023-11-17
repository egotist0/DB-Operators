#pragma once

#include <limits>

namespace buzzdb {

#define UNUSED_ATTRIBUTE __attribute__((unused))

constexpr uint64_t INVALID_PAGE_ID = std::numeric_limits<uint64_t>::max();

constexpr uint64_t INVALID_FRAME_ID = std::numeric_limits<uint64_t>::max();

constexpr uint64_t INVALID_NODE_ID = std::numeric_limits<uint64_t>::max();

constexpr size_t REGISTER_SIZE = 16 + 1;  // null delimiter

}  // namespace buzzdb
