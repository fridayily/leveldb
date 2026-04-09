// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

/*
 * 将 memtable 中的数据写入 ldb 文件
 *
    调用栈
 *  leveldb::BuildTable(const std::string &, leveldb::Env *, const leveldb::Options &, leveldb::TableCache *, leveldb::Iterator *, leveldb::FileMetaData *) builder.cc:32
    leveldb::DBImpl::WriteLevel0Table(leveldb::MemTable *, leveldb::VersionEdit *, leveldb::Version *) db_impl.cc:587
    leveldb::DBImpl::CompactMemTable() db_impl.cc:641
    leveldb::DBImpl::BackgroundCompaction() db_impl.cc:849
    leveldb::DBImpl::BackgroundCall() db_impl.cc:831
    leveldb::DBImpl::BGWork(void *) db_impl.cc:820
    leveldb::PosixEnv::BackgroundThreadMain() env_posix.cc:1014
    leveldb::PosixEnv::BackgroundThreadEntryPoint(leveldb::PosixEnv *) env_posix.cc:865
 */
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "BuildTable begin");
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();  // skip_list 的迭代器

  // note: 将要写入 ldb 文件名
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    // 从imm 中获取第一个key,即最小的 key
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      // 返回 internal key= key_length(varint) + key + value_type + seq + val_length(varint) + val
      // 即 key 和 value 是在一起的
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      // 循环遍历完之后，迭代器的最后一个key最大
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    // 将 table 写入文件
    s = builder->Finish();
    if (s.ok()) {
      // 数据库文件总大小
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                         "BuildTable Success and create Iterator to verify table is usable");
      // note: 读取表的索引信息构建 table, 并存入 table_cache
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
      s = it->status();
      SPDLOG_LOGGER_INFO(SpdLogger::Log(),"delete table_cache TwoLevelIterator");
      delete it;

    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "BuildTable end");
  return s;
}

}  // namespace leveldb
