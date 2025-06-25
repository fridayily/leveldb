// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);  // arg1是一个cache对象
  Cache::Handle* h =
      reinterpret_cast<Cache::Handle*>(arg2);  // arg2是一个handle
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create TableCache");
}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];  // sizeof(file_number)
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(
      key);  // 在缓存中查找key,即ldb 文件的编号，value 是 Table 类型
  if (*handle ==
      nullptr) {  // 如果 Cache 中找不到key，从文件中查找 .ldb 文件打开
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {  // 如果打开失败，尝试打开 SST 文件
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {  // 打开文件时构造了 index_block,meataindex_block,
                   // filter_block
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;  // 包含 table 和 file 实例S
      tf->file = file;                      // 初始化
      tf->table =
          table;  //  插入的 key 为 ldb 文件的id ,value 为 TableAndFile 实例
      *handle = cache_->Insert(
          key, tf, 1, &DeleteEntry);  // TableCache::Get中会调用 cache 中的tf
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size,
                       &handle);  // 从cache或文件中查找对应的表
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  // handle 是插入 file_numble,TableAndFile 到 cache 中后返回的实例，这里取出
  // table
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result =
      table->NewIterator(options);  // 一个二级迭代器，一个迭代index,一个data
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s =
      FindTable(file_number, file_size, &handle);  // 将查到的结点存到handle中
  if (s.ok()) {                                    // t 是存存ldb 文件的 Table
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg,
                       handle_result);  // k 是 InternalKey,arg 是 handle_result
                                        // 函数的参数之一,用于存储
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
