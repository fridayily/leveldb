// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"

#include "leveldb/db.h"

#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "WriteBatch Construct");
  Clear();
}

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);  // 待插入的数据
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  // 待插入的一批数据中 sequence + cnt + [KeyType + keylen + key + vlen + v] +
  // [KeyType + keylen + key + vlen + v]
  input.remove_prefix(kHeader);  // 去除 8 字节的sequence + 4 字节count
  Slice key, value;
  int found = 0;
  while (!input.empty()) {  // 如果 input 存有存键值，进入循环
    found++;
    char tag = input[0];
    input.remove_prefix(1);  // 去掉 tag
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(
                   this)) {  // found 的个数应该等于 writebatch 中的个数
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() +
                       8);  // 移动8字节，跳过 sequence,接下来 4 字节存长度
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);  // 修改长度
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(
      DecodeFixed64(b->rep_.data()));  // 取出 8 个字节的 sequence
}
// 写入序列号，
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) +
                                         1);  // 取出旧的长度加1，然后替换旧长度
  rep_.push_back(
      static_cast<char>(kTypeValue));  // push_back 只能向字符串末尾添加一个字符
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Add {} {} to mem", key.ToString(),
                       value.ToString());
    mem_->Add(sequence_, kTypeValue, key, value);  // 存入 一个Internal key
    sequence_++;                                   // 每存一次，sequence +1
  }
  void Delete(const Slice& key) override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Delete {} from mem", key.ToString());
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;  // memtable插入器
  inserter.sequence_ =
      WriteBatchInternal::Sequence(b);  // 前 8 个字节存 sequence_
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);  // MemTableInserter 继承自 WriteBatch::Handler
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);  // 移除 KHeader，添加KHeader 之后数据
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
