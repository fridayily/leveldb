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

/*
 * 里面存储一些元信息用来辅助读取 data_block
 * 如 index_block 存储了索引信息，可以定位到 file 中的 data_block
 * filter_data 存储了 bloom filter 信息，用来判断 key 是否存在
 */
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

/*
 * 根据 file 中的数据创建 Table
 *    Table 只有一个私有成员变量 Rep
 *    Rep 包含 index_block,filter_block 用来辅助读取 file 中真实的 kv 数据
 *    Table 类的所有方法都通过 rep_ 指针访问实际数据
 */
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                     "-----------open table-----------------");

  *table = nullptr;
  if (size < Footer::kEncodedLength) {  // Footer 大小固定，小于此值错误
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 将 footer 读取到 footer_space 中
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "read footer");
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  // 反序列化footer，写入index_block,meta_index 偏移信息
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {  // 偏执的检查
    opt.verify_checksums = true;
  }
  // 读取 index_block
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "read index block");
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "build index block");
    // 根据上面读取的index_block创建index_block
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    // 获取 metaindex_block 的索引
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "cache_id {}", rep->cache_id);

    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "read meta to table");
    (*table)->ReadMeta(footer);  // 构造 filter_block
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  // mataindex block 存储的是 filter block的索引信息，没有 filter_policy
  // 则不用读取
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
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "read metaindex content");
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create metaindex block");
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete meta block iter");
  delete iter;
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete meta block");
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;  // 获取 filter_block 的 offset和 size
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
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"delete block");
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
// 里面包含 ldb 文件的id, 如果 cache 中找不到，则在 ldb 文件中读取对应的 data_block
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  // 传入的参数就是 Table* 型的，现在转回去
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "BlockReader beg");

  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  // data_block的 offset size 信息，解析出 offset 和 size
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      // 将 cache_id 和 offset 封装成一个 key
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      // 读取某个数据块时，先检查数据块是否在 block_cache 中
      // 这里的key由 cache_id 和 offset 组成
      cache_handle = block_cache->Lookup(key);
      // 如果在 block_cache 找到，直接从缓存块中取得数据块
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 如果数据块不在缓存中，从文件中读取
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          // contents 只有数据，构造 block 后方便迭代
          block = new Block(contents);
          // ReadBlock 时将 contents.cacheable 设置为False 为什么？
          if (contents.cachable && options.fill_cache) {
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
        // 根据 content 内容设置 size_，restart_offset_
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;          // 重新构造个迭代器
  if (block != nullptr) {  // data_block 的迭代器
    iter = block->NewIterator(
        table->rep_->options.comparator);  // 这里的data_block
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);  // 注册清除函数
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "BlockReader end");
  return iter;
}
// BlockReader是一个函数（arg，options，index_value）， const_cast<Table*>(this) 就是 arg
Iterator* Table::NewIterator(const ReadOptions& options) const {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create NewTwoLevelIterator of table");
  return NewTwoLevelIterator(
      // 二级迭代器，传入的参数是迭代器，返回的是迭代器
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

/*
 * 用于从 SST 表中根据给定的 key 查找对应的 value
 * 调用链  DBImpl::Get → Version::Get → TableCache::Get → Table::InternalGet
 *
 * 即              db_->Get(options, k, &result)
 *      --------→ current->Get(options, lkey, value, &stats)
 *      --------→ state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue)
 *      --------→ t->InternalGet(options, k, arg,handle_result)
 *
 * 在 Version::Get 中，创建了一个 Saver 结构体，并将 SaveValue 作为回调函数传递给
 * TableCache::Get，最终传递给Table::InternalGet。
 * 当Table::InternalGet找到匹配的键值对时，就会调用这个回调函数来处理结果。
 *
 * note: 这里的 handle_result 是 SaveValue
 *       arg 是 State::Saver, 里面存储要查找的 key, 定义的比较器，和用来保存值的指针
 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  // 创建索引块（index block）的迭代器
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  /*
   * 定位到第一个大于等于 k 的位置
   * data_block 中如果存的是 a1,a2,a3, 对应的 index_block 存的是 b (b 比 a3 大)
   * 假设要查找 a3, itter->Seek(a3) 会返回 b 所在 index_block 的 handle
   * 根据这个 handle 可以定位到 a3 所处的 data_block
   */
  iiter->Seek(k);
  if (iiter->Valid()) {
    // 取出index 迭代器的 offset size
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found 执行这里说明在 bloo 过滤器中没有找到，直接返回
    } else {
      // 没有设置过滤器或者 bloom 过滤器中查到
      // 根据 index_block 得到的迭代器值获得 data_block 迭代器
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      // 定位到 k 的位置
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        // 如果找到匹配的 key-value，则调用回调函数 handle_result 处理结果
        // 调用 SaveValue(State::Saver,key,value)
        // 如果 arg 的 key 和 block_iter->key() 相同
        // 则设置 tate::Saver::SaverState 为 kFound 和 tate::Saver::value
        // note: InternalGet 这个函数自身没有返回查找的值，
        //      而是修改 Version::Get 中的 value
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

// 返回的值就是 key 所处 data_block 的偏移
// 即对于同一个 data_block 的key, ApproximateOffsetOf(key) 的返回值相同
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
      result = handle.offset();  // 返回的值就是 key 在 table 中的偏移
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
