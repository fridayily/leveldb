// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

//#include <iostream>
namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};

// 重载 operator<<
//std::ostream& operator<<(std::ostream& os, RecordType recordType) {
//  switch (recordType) {
//    case RecordType::kZeroType:
//      os << "kZeroType";
//      break;
//    case RecordType::kFullType:
//      os << "kFullType";
//      break;
//    case RecordType::kFirstType:
//      os << "kFirstType";
//      break;
//    case RecordType::kMiddleType:
//      os << "kMiddleType";
//      break;
//    case RecordType::kLastType:
//      os << "kLastType";
//      break;
//    default:
//      os << "Unknown";
//      break;
//  }
//  return os;
//}


static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768; // 32K

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
