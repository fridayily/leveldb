// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }
  // 取一个大于start。小于 limit的字符串  (k01,k03)->k02  (k03,k04)->k03
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    // 查找公共前缀
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) { // 比较start limit 相同的字符数
      diff_index++; // 这里是第一个不相同字符的索引
    }

    if (diff_index >= min_length) { // 相同的字符数超过最短key长度
      // Do not shorten if one string is a prefix of the other
    } else {
      // 比较第一个不相等的字符,将不相同字符转成int
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      // 不相等的字节小于 0xff 不相同字符的差值大于1
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        // 差异字符+1
        (*start)[diff_index]++;
        // 重置字符串大小，只取前面 diff_index+1 个字节
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    // 返回比上一个key大1的字符串 如 abcd->b
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

// C++ 标准规定函数级静态变量的析构函数在程序终止时调用
// 这导致这种行为有时会导致非常奇怪且难以追踪的问题
// 通常会尽量避免使用具有复杂析构函数的静态函数级变量
// static function-level
// C++11 标准规定了局部静态变量的初始化是线程安全的
const Comparator* BytewiseComparator() {
  // 局部静态对象声明
  // 在创建 Comparator 时实例化，在程序结束时析构
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
