// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}
/* 初始 record 和 scratch 都为空 */
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"");
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size(); // fragment 为取出的数据
    //  (总共可读的长度 - 剩余可读长度 - 本次读取长度  ) = 上次读取的长度累计
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        // TEST_F(LogTest, UnexpectedFullType)
        // 如果前一次碰到了 kFirstType, in_fragmented_record 被设置为 true
        // 再次读取又碰到了 kFullType ，则出现异常
        // 之前读取的数据放在 scratch 中，这里会报告 ReportCorruption
        //
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        // 记录上次读取长度累计值
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        // 存储本次读取数据
        *record = fragment; // 数据存入 record
        last_record_offset_ = prospective_record_offset; // 上一次读取记录的偏移量
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}
/* result初始为空指针, 该函数返回一个 record 指向该指针 */
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // 第一次读取时 buffer 为空，符合条件，读取 kBlockSize 字节数据到 buffer_ 中
    // 当buffer_读取完毕后，如果继续读取，也会进入，碰到 EOF,返回 KEof
    //
    if (buffer_.size() < kHeaderSize) { // buffer_ 为全局的,记录了剩余要读的数据 ,第一次 size 为0
      if (!eof_) {  // 碰到 eof 标志
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();  // 如果 contents_ 里面数据较少,会读出全部数据
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_); // 读取一整个 KBlockSize
        end_of_buffer_offset_ += buffer_.size(); // buffer_ 是 read 读出来的
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) { // 读出来的数据少于 KBlock 数据,说明全部读出,则设置结束符
          eof_ = true;
        }
        continue; // 若成功读出数据，buffer_.size()大于 kHeaderSize 跳出 while 循环
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header  读出了有效数据，解析 header
    const char* header = buffer_.data(); // buffer_ 包含的这个数据,header 指针指向开始位置
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff; // 日志长度
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff; // 日志长度
    const unsigned int type = header[6];  // 存储类型 Record Type
    const uint32_t length = a | (b << 8); // 转换数据的长度
    if (kHeaderSize + length > buffer_.size()) { // kHeaderSize + length 是一整条数据的长度  buffer_ 是整个日志block中含数据的长度
      size_t drop_size = buffer_.size(); // 表明一条 record 记录大于剩余可读的数据,表明有异常
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header)); // 固定32字节是CRC
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length); // 为什么是6, 因为 checksum 是 type(RecordType)+data 计算出来的
      if (actual_crc != expected_crc) { // 读取的 checksum 和 重新计算的 crc 是否相同
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length); // 偏移一条 record 的长度
    // end_of_buffer_offset_ 是数据结束位置的偏移量, buffer_.size() 是剩余未读取的大小  (kHeaderSize + length) 为一个record 长度
    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length); // 取出一条数据,没有数据拷贝,指向实际数据地址
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
