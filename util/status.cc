// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"

#include <cstdio>

#include "port/port.h"

namespace leveldb {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  std::memcpy(&size, state, sizeof(size)); // 前面4字节存储大小
  char* result = new char[size + 5]; // 根据上面获得的字节创建新的数据空间
  std::memcpy(result, state, size + 5); // 将整个state拷贝到新的数组
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  char* result = new char[size + 5];
  std::memcpy(result, &size, sizeof(size)); // 将 size 的值存到result 中,size 4 个字节,即前面4字节存放长度信息
  result[4] = static_cast<char>(code); // 第 5 字节存放状态
  std::memcpy(result + 5, msg.data(), len1); // 将msg 存入
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        std::snprintf(tmp, sizeof(tmp),
                      "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type); // 这里是 string 的构造方法
    uint32_t length;
    std::memcpy(&length, state_, sizeof(length)); // 将state_长度拷贝到length
    result.append(state_ + 5, length); // 添加 state_ 5个字节后 长度为 length 的数据到 result
    return result;
  }
}

}  // namespace leveldb
