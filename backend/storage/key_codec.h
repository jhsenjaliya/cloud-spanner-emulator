//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_KEY_CODEC_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_KEY_CODEC_H_

#include <string>

#include "backend/datamodel/key.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Encodes a Key into a byte string that preserves the sort order defined by
// Key::Compare(). The encoding handles ascending/descending columns and
// NULL ordering (nulls-first vs nulls-last).
//
// Encoding format per column:
//   NULL:      0x00 (nulls-first) or 0xFE (nulls-last)
//   BOOL:      0x01 (false) or 0x02 (true)
//   INT64:     0x03 + 8 bytes big-endian with sign bit flipped
//   DOUBLE:    0x04 + 8 bytes IEEE 754 with sign manipulation
//   STRING:    0x05 + byte-stuffed UTF-8 with 0x00 0x00 terminator
//   BYTES:     0x06 + byte-stuffed bytes with 0x00 0x00 terminator
//   TIMESTAMP: 0x07 + 8 bytes seconds + 4 bytes nanos (big-endian)
//   DATE:      0x08 + 4 bytes big-endian with sign bit flipped
//   NUMERIC:   0x09 + serialized numeric bytes + 0x00 terminator
//   Infinity:  0xFF
//
// For descending columns, all bytes are bitwise-inverted.
std::string EncodeKey(const Key& key);

// Encodes a Key that represents a prefix limit. The encoded form will sort
// after any key with the same prefix. This is achieved by appending 0xFF
// bytes to the prefix encoding.
std::string EncodeKeyForPrefixLimit(const Key& key);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_KEY_CODEC_H_
