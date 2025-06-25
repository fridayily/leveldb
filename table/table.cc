// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"-----------open table-----------------");

  *table = nullptr;
  if (size < Footer::kEncodedLength) {  // Footer 大小固定，小于此值错误
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 将 footer 读取到 footer_space 中
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"read footer");
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input); // 反序列化footer，写入index_block,meta_index 偏移信息
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) { // 偏执的检查
    opt.verify_checksums = true;
  }
  // 读取 index_block
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"read index block");

  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"build index block");
    // 根据上面读取的index_block创建index_block
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle(); // 获取 metaindex_block 的索引
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"cache_id {}",rep->cache_id);

    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer); // 构造 filter_block
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  // mataindex block 存储的是 filter block的索引信息，没有 filter_policy 则不用读取
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"read metaindex content");
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"create metaindex block");
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle; // 获取 filter_block 的 offset和 size
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 当读取某个数据块时，先检查是否在block_cache中，存在就直接读取
// 不存在就读取文件，然后在存入block_cache 中
// arg 是 table 类型
// 里面包含 cache_id , 这个id + offset 组成 cache 的key
//        根据这个 key 在 block_cache 中查找对应的 block 是否存在
// 里面包含 ldb 文件的id, 如果 cache 中找不到，则在ldb文件中读取对应的data_block
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg); // 传入的参数就是Table*型的，现在转回去
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value; // data_block的 offset size 信息
  Status s = handle.DecodeFrom(&input); // 解析出 offset 和 size
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      // 读取某个数据块时，先检查数据块是否在block_cache中
      cache_handle = block_cache->Lookup(key); // 这里的key由cache_id 和 offset 组成
      // 如果在 block_cache 找到，直接从缓存块中取得数据块
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 如果数据块不在缓存中，从文件中读取
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents); // contents 只有数据，构造 block 后方便迭代
          if (contents.cachable && options.fill_cache) { // ReadBlock 时将 contents.cacheable 设置为False 为什么？
            // 从文件中读取成功后将block存入 block_cache中，下次读取时直接读取
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      // file 存的是数据，handle 存是索引信息，根据 handle 将数据写到 contents
      // ReadBlock 中有对文件的数据的合法性进行校验
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents); // 根据content 内容设置 size_，restart_offset_
      }
    }
  }

  Iterator* iter; // 重新构造个迭代器
  if (block != nullptr) { // data_block 的迭代器
    iter = block->NewIterator(table->rep_->options.comparator); // 这里的data_block
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr); // 注册清除函数
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}
// BlockReader是一个函数（arg，options，index_value），
Iterator* Table::NewIterator(const ReadOptions& options) const {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"create NewTwoLevelIterator of table");
  return NewTwoLevelIterator( // 二级迭代器，传入的参数是迭代器，返回的是迭代器
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator); // index_block 的迭代器
  iiter->Seek(k); // 定位到第一个大于等于k的位置
  if (iiter->Valid()) {
    Slice handle_value = iiter->value(); // 取出index 迭代器的 offset size
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found 执行这里说明在 bloo过滤器中没有找到，直接返回
    } else {
      // 没有设置过滤器或者 bloom 过滤器中查到
      Iterator* block_iter = BlockReader(this, options, iiter->value()); //根据 index_block 得到的迭代器值获得data_block迭代器
      block_iter->Seek(k); // 定位到k 的位置
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// 返回的值就是 key 在 table 中的偏移
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  // 这里是index_block 的迭代器,故在 index_block 中查找指定的 key
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    // 存的是 offset size
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset(); // 返回的值就是 key 在 table 中的偏移
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
