// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}
// https://hur.st/bloomfilter
/*
 *  k hash 函数个数
 *  n 过滤器中元素个数
 *  P 误判率
 *  m 过滤器长度
 * */
class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override { // keys 长度为n的数组
    // Compute bloom filter size (in both bits and bytes)
    size_t bits = n * bits_per_key_; // bits_per_key_ 在初始化时确定,n 是keys 数组长度

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64;

    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t init_size = dst->size();
    //这里将数据全部填充为 0，所以 push_back 将k_ 添加到最后，新增一个元素
    // dst 的最大长度为 init_size + bytes，原始值不变，剩余的用 0 填充
    // dst 的值是一个指针，指向一个 string, resize 是对指向的 string 进行操作
    // 即 初始化 和 在最后面添加 k_
    dst->resize(init_size + bytes, 0); // dst 是指针, hash 数组就存在这里,这里确定了 hash 表的大小
    dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter 保存hash 函数个数
    char* array = &(*dst)[init_size]; // dst 有个初始长度，后面会加上 bytes 字节用于放置 hash 数组 ，这里定位到放hash 的最开始的位置
    for (int i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      uint32_t h = BloomHash(keys[i]); // 取出第i个key，进行 hash
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      for (size_t j = 0; j < k_; j++) { // 一共进行 k_ 次 hash,每次都将结果存入 array 中
        const uint32_t bitpos = h % bits; // bloom 滤波器长度为bits，即第几个bit ,通过下面代码设置该bit为1，每一个key经过6次hash，设置6个bit，
        array[bitpos / 8] |= (1 << (bitpos % 8)); // 这里采用 或 运算
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
    const size_t len = bloom_filter.size(); // 滤波器大小 + 1字节的 hash 函数个数
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len - 1]; // hash 次数在数组最后
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      // 这里是相与,相同才为1
      // 任何一次hash 不命中则说明 key 不在其中  array的第 bitpos / 8 位和 新产生的 1 << (bitpos % 8) 相同
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;
  size_t k_;
};
}  // namespace

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
