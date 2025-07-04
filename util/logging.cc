// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/logging.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <limits>

#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(num));
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') { // ascii 0x20 到 0x7e 可见字符
      str->push_back(c);
    } else {
      char buf[10];
      // %02x 表示以两位十六进制数的形式输出，不足两位时前面补零
      // static_cast<unsigned int>(c) 将字符 c 转换为无符号整数类型
      // & 0xff 确保只取低8位，适用于处理字符或字节数据
      std::snprintf(buf, sizeof(buf), "\\x%02x",
                    static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}
// 提取文件名的数字到 val 中
bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  // Constants that will be optimized away.
  constexpr const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
  constexpr const char kLastDigitOfMaxUint64 =
      '0' + static_cast<char>(kMaxUint64 % 10);
  // static_cast<char>(kMaxUint64 % 10) 的ascii 码为5，加上'0' 为 53,即'5'
  uint64_t value = 0;

  // reinterpret_cast-ing from char* to uint8_t* to avoid signedness.
  const uint8_t* start = reinterpret_cast<const uint8_t*>(in->data());

  const uint8_t* end = start + in->size(); // 开始地址 + 字符串大小 = 结束地址
  const uint8_t* current = start;
  for (; current != end; ++current) {
    const uint8_t ch = *current;
    if (ch < '0' || ch > '9') break;

    // Overflow check.
    // kMaxUint64 / 10 is also constant and will be optimized away.
    // 如果继续执行,下面 value 会 * 10, 会溢出,所以提前终止
    if (value > kMaxUint64 / 10 ||
        (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUint64)) {
      return false;
    }
    // 将字符串数字转为数字 '123'
    // 0*10 + '1'-'0'
    // 1*10 + '2'-'0'
    // 12*10 + '3' - '0'
    value = (value * 10) + (ch - '0');
  }

  *val = value;
  const size_t digits_consumed = current - start; // 数据长度
  in->remove_prefix(digits_consumed);//移除前面几位
  return digits_consumed != 0;
}

}  // namespace leveldb
