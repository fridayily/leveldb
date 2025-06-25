// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    // 将index_block 的重启点设置为1，该参数是全局变量，option 中默认值为 16
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  // 写完一个data_block后，不会立即写index_block的索引条目，而是等到下一个data_block写入时才写
  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  // "the q" << "the r" << "the w"  索引条目the r在数据条目之间
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// 将数据添加到 data_block 中
// 如果调用上一次 Add 时 data_block 写满了,即超过 block_size
// 会生成一个完成的 data_block
// 这次 Add 时会添加一个 k,v 到 index_block

// 如果设置的 filter_block ,会添加到 filter_block 中
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // 如果之前有存入key，判断新 key 是否比上一个key 大
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  // 如果上一个 data_block 写完了，就将上一个data_block
  // 的索引条目添加到index_block 中
  if (r->pending_index_entry) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "pending index entry");
    // 之前的 data_block 数据写完清空，现在是空的block
    assert(r->data_block.empty());
    // 找一个比上次写到 data_block 的key 大的 key 作为 index_block 的 key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // pending_handle 存储的是 data_block 的 offset,size
    std::string handle_encoding;
    // index_block 的key是上一个data_block的key
    // (一般介于上一个data_block的key和下一个data_block的key之间)
    // 数据是 data_block 的 siz e和 offset
    r->pending_handle.EncodeTo(&handle_encoding);

    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "index block add key {}", r->last_key);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "filter block add key {}",
                       key.ToString());
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "data block add key {} value {}",
                     key.ToString(), value.ToString().substr(0, 10));
  // data_block 添加数据
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                       "estimated_block_size {} > options.block_size {} will "
                       "flush a data block",
                       estimated_block_size, r->options.block_size);
    // 这里将 data_block 写到文件,但并没有写入 index_block ,index_block 还没生成
    // 即 每生成一个data_block 都及时到写到了文件
    // 而对应的索引一直在内存中
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write data block");
  // 在 block 后面添加 type 和 crc
  // 初始时 pending_handle 为空
  // 这里通过 data_block 计算 offset 和 size 来设置 pending_handle
  // 但并没有写到 rep 中,而是成功后将 pending_index_entry 设为 true
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    // 将 data_block 写入文件
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

// BlockBuild 可以是 data_block index_block
// TableBuild 包含 data_block,index_block filter_block metaindex_block
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Create a Block");
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 存入restart数组信息, 标记 block 结束 ,raw 了 block 的数据
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // 对 key value 的数据 + 重启点数组进行压缩
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //  printf("WriteRawBlock add type crc\n");
  // 添加 type CRC
  // 对压缩后的数据 + 压缩类型 计算 CRC
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();  // 每写入一个block 清空
}

// 1.在 block_contents 后面添加 type 和 crc
// 2.将 size 和 offset 存到 handle 中
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  // handle用来记录每次写入block后的偏移,比如入写入data_block后再写入metaindex_block
  handle->set_size(block_contents.size());  // data_block/metaindex_block 的大小
  // 写入 Block 数据到文件,小数据是写到缓存
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    // Extend crc to cover block type  加入block type，重新计算crc
    crc = crc32c::Extend(crc, trailer, 1);
    // 将 CRC32 添加到 trailer
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 修改 offset
      // 上面数据写到缓冲区后，更新偏移量
      // rep 的 offset 中记录的是所有 block 的偏移量的和
      // 所以 index_block 中不同 entry 的 offset 是递增的,而不是每个从 0 开始
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Finish");

  Rep* r = rep_;
  Flush();  // 将 data block 写入文件
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block，将滤波器数据写入f 缓冲区中， 并更新r.offset
  if (ok() && r->filter_block != nullptr) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write filter block");

    // filter_block->Finish() 构建滤波器数据
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block   metaindex
  // 没有数据，包含一个2个字节的重启点字节+1字节压缩类型+4字节crc
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data6
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(
          &handle_encoding);  // filter_block的offset 和 size
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "meta index block add {}", key);

      meta_index_block.Add(key, handle_encoding);
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write meta block");

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(
          &r->last_key);  // 找到一个比上一次存储的key大1点的key abcd->b
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);  // 添加offset  size
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "index block add {}", r->last_key);

      // index_block 记录了不同 data_block 的索引信息
      // 此时 data_block 都写到了磁盘
      // 这里完成最后 index_block 的添加后才将 index_block 写到磁盘
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    //    printf("index_block write...\n");
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write index block");
    // 这里是添加重启点信息、压缩信息、CRC
    // 整个代码只有这里对 index_block 有这样的操作
    // 所以一个 ldb 文件只有一个 index_block
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write footer");
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
