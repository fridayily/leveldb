// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}
// 解析出offset_ 和 size_
Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size(); // 这里footer 的大小固定48字节
  metaindex_handle_.EncodeTo(dst); // 编码metaindex的偏移和长度
  index_handle_.EncodeTo(dst); // 编码index的偏移和长度
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding   // 0xdb4775248b80fb57ull
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu)); // 0x8b80fb57 取低8位
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32)); // 0xdb477524
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8; // 魔数地址
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input); // 将 offset_,size 添加到 meta_inde_block
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input); // 将offset_,size 添加到 index_handle
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8; // 魔数的指针+8字节偏移 指向 结束位置
    *input = Slice(end, input->data() + input->size() - end);  // input 开始指向 Footer padding 开始部分，加上size指向结束部分，最后input 指向结束部分
  }
  return result;
}
// 从文件中读取，所以 cachable，heap_allocated 为false,将结果放入 result 中
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size()); // 返回 block 的大小
  char* buf = new char[n + kBlockTrailerSize]; // block + 压缩类型 + crc 后的总大小
  Slice contents;
  // 将数据读取到 buf 指向的内存中, contents 同样指向 buf 的地址，只是数据类型解释为 Slice
  // 指定block的偏移地址，要读取的大小，block 可能包含多个k,v
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    // data + n + 1 后面四个字节存的是 CRC32
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1); // 根据数据计算 CRC32
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) { // n 是最后一位，标识压缩类型
    case kNoCompression:
      if (data != buf) { // 如果数据在缓存中，data=buf,如果数据在文件中，data!=buf
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        // 读取的数据存放在data中，没必要使用 buf ,删除
        delete[] buf;
        // 文件中读取的数据,即一个重启点指定的数据,
        // 由多个[shared,non_shared,value_size,key_non_shared,value] 组成
        result->data = Slice(data, n);
        result->heap_allocated = false; //
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
