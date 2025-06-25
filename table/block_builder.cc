// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"

#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}
// 假如有3个重启点，会用3个 uint32 记录重启点偏移量 外加一个节点记录重启点的个数
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

// 添加 restart_ 数组，大小  ，标记为 finished 为 true
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
//    printf("---restarts_:%u----\n",restarts_[i]);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "add restarts offset {}",restarts_[i]);
    PutFixed32(&buffer_, restarts_[i]);
  }
//  printf("---restarts_.size():%zu----\n",restarts_.size());
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "add restarts_.size() {}",restarts_.size());
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);  // 上一个存储的key
  assert(!finished_);
  // 判断存入的 key 的数量是否超过间隔数量
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {  // 设置重启点的间隔
    // See how much sharing to do with previous string
    // 取上一个存储的key 和当前key 中最小的长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    // 计算两个key的相同的长度
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;  // 共享的key的长度
    }
  } else {
    // Restart compression
    restarts_.push_back(
        buffer_.size());  // 将已存储的大小保存到 restarts_ vectory 中
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"add restarted point and size is {}",restarts_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;
  // 要存储的 key 减去共享的长度后的非共享的长度,如果进入新的重启点，shared还是0

  // Add "<shared><non_shared><value_size>" to buffer_  buffer_ 初始为空，要存储
  // data_block,index_block 等
  PutVarint32(&buffer_, shared);        // 共享的长度
  PutVarint32(&buffer_, non_shared);    // 非共享的长度
  PutVarint32(&buffer_, value.size());  // 值的大小

  // Add string delta to buffer_ followed by value
  // 移动key的指针，将非共享的key保存到buffer
  buffer_.append(key.data() + shared,non_shared);
  buffer_.append(value.data(), value.size());  // 保存数据

  // 输出buffer
//  printf("buffer___,counter_ %d buffer_.size():%zu value.size:%zu\n", counter_,
//         buffer_.size(), value.size());
//  if (value.size() < 20) {
//    // 这里如果是char 会多出很多ffff字符
//    for (unsigned char c : buffer_) {
//      printf("%02x", c);
//    }
//    printf("\n");
//  }

  // Update state 刚刚存储的key
  last_key_.resize(shared);  // last_key_ 取前面 shared 位
  last_key_.append(key.data() + shared,
                   non_shared);  // 将 key.data 的no_shared 添加到last_key_后
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
