// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"

#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  // 通过 index_block 可以确定 data_block 的 offset 和 size
  // index_block 里面可以分为多个重启点，每个重启点都可以确定一个 data_block
  // 每个 data_block 又可以可以分为多个重启点
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // data_block_handle_ 存储了 data_block 的 offset 和 size
  // 该值存在 index_block 中
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),  // index 迭代器
      data_iter_(nullptr) {}    // 数据迭代器

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToFirst of index iter begin");
  index_iter_.SeekToFirst();  // 取出index_block的第一个key
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "InitDataBlock");
  InitDataBlock();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                     "InitDataBlock success, and seek to first of data block");

  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SkipEmptyDataBlocksForward");
  SkipEmptyDataBlocksForward();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToFirst of index iter end");
}

void TwoLevelIterator::SeekToLast() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToLast of index iter begin");
  index_iter_.SeekToLast();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "InitDataBlock");
  InitDataBlock();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                     "InitDataBlock success, and seek to last of data block");
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToLast of index iter end");

}

void TwoLevelIterator::Next() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "two level iter next");
  assert(Valid());
  data_iter_.Next();
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SkipEmptyDataBlocksForward");
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}
// 检查data_iter 是否有效，data block 读完后进入该函数
// 第一次进入后 index_iter 是有效的
// 会执行 index_iter.Next()
// 如果 index_iter 有效，说明还有 data block 可以读取，则初始化 data block
// 如果 index_iter 无效，说明没有 data block 可以读取，设置 data iter 无效
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "begin");
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                         "index iter is invalid and set data iter to null");
      SetDataIterator(nullptr);
      return;
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "next index iter and init data block");
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "end");
}

// 当data 迭代器无效时
// 如果此时 index 迭代器也无效，设置 data 迭代器为 nullptr
// 如果index 迭代器可以继续迭代，则重新构建 data 迭代器
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}
// 索引迭代器无效而数据迭代器有效则抛出错误
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}
// 两级迭代器，以及索引index block,二级是 data block
void TwoLevelIterator::InitDataBlock() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "begin");
  if (!index_iter_.Valid()) {  // 索引迭代器无效，则将数据迭代器设为NULL
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                       "index iter invalid, set data iter to null");
    SetDataIterator(nullptr);
  } else {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "index iter valid, init data block");
    Slice handle =
        index_iter_.value();  // index_iter的value 指向data_block的索引
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {  // data_block 的 handle 是存储在 index_block 中
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
