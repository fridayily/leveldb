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
  // arg1是一个cache对象, arg2是一个handle
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options, int entries)
    : env_(options.env), dbname_(dbname), options_(options), cache_(NewLRUCache(entries)) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create TableCache");
}

TableCache::~TableCache() { delete cache_; }

/*
 * 每次写入 ldb 文件都会重新打开，验证 ldb 文件是否合法
 * 并解析 ldb 文件的索引信息实例化一个 TableFile 实例，并将其存到 TableCache 中
 * 这样后续查找时可以避免重复解析索引信息
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "FindTable: add tablefile to TableCache");
  Status s;
  char buf[sizeof(file_number)];  // sizeof(file_number)
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  // 在缓存中查找 key, 即 ldb 文件的编号，value 是 Table 类型
  *handle = cache_->Lookup(key);
  // 如果 Cache 中找不到key，从文件中查找 .ldb 文件打开
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 如果打开失败，尝试打开 SST 文件
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // 打开文件时构造了 index_block,metaindex_block,filter_block
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      // 包含 table 和 file 实例
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 插入的 key 为 ldb 文件的 id ,value 为 TableAndFile 实例, 该实例已经解析了ldb 文件的索引信息
      // TableCache::Get中会调用 cache 中的 tf
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number,
                                  uint64_t file_size, Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  // 从cache或文件中查找对应的表
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  // handle (LRUHandle 类型) 是插入 file_number, TableAndFile 到 cache 中后返回的实例，这里取出 table
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  // 一个二级迭代器，一个迭代index,一个data
  Iterator* result = table->NewIterator(options);
  /*
   * 将 handle 加入一个 cleanup_head_ 中
   * 当迭代器析构时，会自动调用所设置的删除函数
   */
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
                       const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  // 将查到的结点存到 handle中
  // Cache::Handle 使用了不透明指针（Opaque Pointer）设计模式
  // 必须通过 cache_->Value(handle) 之类操作获取里面的值
  // 即用户不会感知到内部的实现细节
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    // t 是存存ldb 文件的 Table
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    // k 是 InternalKey,arg 是 handle_result 函数的参数之一,用于存储
    s = t->InternalGet(options, k, arg, handle_result);
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
