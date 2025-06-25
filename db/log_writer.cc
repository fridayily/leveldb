// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"");

  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;
// 将要写的数据添加到log 中，32K一个block
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {  // 如果上一次刚好写完一个block,还有数据要写，这里 leftover=0
    const int leftover =
        kBlockSize -
        block_offset_;  // kBlockSize 默认大小 32K, block_offset_
                        // 记录已保存的日志偏移量，这里计算当前block 可用长度
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {  // leftover= kHeaderSzie 是依然可以写个""
      // Switch to a new block
      if (leftover >
          0) {  // 如果剩余的空间大于0 小于 KHeaderSize,将剩余空间用0填充
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;  // 一个 kBlockSize 写完重置偏移量
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 不允许剩余空间小于 kHeaderSize
    assert(kBlockSize - block_offset_ - kHeaderSize >=
           0);  // block_offset_ 可能是初始化的0,可能是上一行重置

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;  // 剩余可用
    const size_t fragment_length =
        (left < avail)
            ? left
            : avail;  // left
                      // 是数据长度，如果要写的长度大于可用的空间，拆分片段写
    // 如果 left > avail, 将 left 拆分成 fragment
    RecordType type;
    const bool end =
        (left ==
         fragment_length);  // 如果还有剩余的要写,说明不是end,否则设end 为 true
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;  // 要写的数据长度大于剩余可写的
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(
        type, ptr, fragment_length);  // 每次写入data的长度或者 剩余可写空间长度
    ptr += fragment_length;           // ptr 可能是个很长的字符串,每次最多只处理
                                      // kBlockSize 字符
    left -= fragment_length;  // left 可能大于 kBlockSize,表示剩余要写长度
    begin = false;            // 如果 kBlockSize 不够写,则置false
  } while (s.ok() && left > 0);  // left >0 说明还有未写的数据
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  //    spdlog::info("EmitPhysicalRecord {}", int(t));
  //  std::cout << "EmitPhysicalRecord " << t << std::endl;
//  SpdLogger::Log()->debug("EmitPhysicalRecord {}", int(t));
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),int(t));
  assert(length <= 0xffff);  // Must fit in two bytes // 小于 64K
  assert(block_offset_ + kHeaderSize + length <=
         kBlockSize);  // 是否有足够的空间写

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);  // 长度低位
  buf[5] = static_cast<char>(length >> 8);    // 长度高位
  buf[6] = static_cast<char>(t);              // 状态

  // Compute the crc of the record type and the payload.
  // type_crc_[]是已经计算好的    type 和 data 是一起计算 crc 的
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);  // 计算数据的crc
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);  // 将crc 存入buf的前面4字节

  // Write the header and the payload
  // 写入空字符串时也是要写KHeaderSize在写0长度数据
  // 将kHeader 写到 dest_.buf_ 中，缓冲区写满后写文件
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));  // 写Data到dest_.buf_
    if (s.ok()) {
      s = dest_->Flush();  // Flush 有多种实现，有的写磁盘，有的直接返回OK
    }
  }
  block_offset_ += kHeaderSize + length;  // 移动写指针
  return s;
}

}  // namespace log
}  // namespace leveldb
