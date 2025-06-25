// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}
// 当block_offset超过kFilterBase 2048 后
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key; // internal_key
  start_.push_back(keys_.size()); // filter_block 的偏移量,keys存储了全部的key
  keys_.append(k.data(), k.size()); // filter_block 的keys
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) { // start_记录的是重启点的偏移量
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size(); // 滤波器大小
  for (size_t i = 0; i < filter_offsets_.size(); i++) { // 滤波器数量
    PutFixed32(&result_, filter_offsets_[i]); // 将第i个滤波器的偏移量添加到result_中
  }

  PutFixed32(&result_, array_offset); // 将滤波器的数据的大小添加到result_
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_); // push_back 一个 size_t 对象，这里长度加1
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size(); // 获取重启点个数
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys); // 将 keys_ 中存储的key 根据存储在 start_中的偏移量提取出来放到 tmp_keys_中
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size()); // result 用于存储滤波器数据，这里存储滤波器数据偏移量
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear(); // 清空临时的keys,存的是 user_key
  keys_.clear(); // keys 存储所有key
  start_.clear(); // start_ 存储每个key的偏移
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1]; // 获取编码参数 kFilterBaseLg 默认11
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); // 取出 array_offset,即 过滤器数据的大小
  if (last_word > n - 5) return;
  data_ = contents.data(); // 过滤器的开始地址
  offset_ = data_ + last_word; // 过滤器结束地址
  num_ = (n - 5 - last_word) / 4; // filter_offset 个数
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_; // 获取过滤器的索引
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4); // 第 index 个filter_offset保存的值
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4); // 第 index +1 个filter_offset 保存的值
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start); // 抽取指定返回的数据，即返回的 index 个过滤器
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
