// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

#include <cstdarg>

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// See env.h for justification.
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#undef DeleteFile
#endif

namespace leveldb {

Env::Env() = default;

Env::~Env() = default;

Status Env::NewAppendableFile(const std::string& fname, WritableFile** result) {
  return Status::NotSupported("NewAppendableFile", fname);
}

Status Env::RemoveDir(const std::string& dirname) { return DeleteDir(dirname); }
Status Env::DeleteDir(const std::string& dirname) { return RemoveDir(dirname); }

Status Env::RemoveFile(const std::string& fname) { return DeleteFile(fname); }
Status Env::DeleteFile(const std::string& fname) { return RemoveFile(fname); }

SequentialFile::~SequentialFile() = default;

RandomAccessFile::~RandomAccessFile() = default;

WritableFile::~WritableFile() = default;

Logger::~Logger() = default;

FileLock::~FileLock() = default;

void Log(Logger* info_log, const char* format, ...) {
  if (info_log != nullptr) {
    std::va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname, bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->RemoveFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}
// 将fname 中的数据读到 data 中
Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear(); // 清除data中数据，data 可能保有之前写入的数据
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file); // 创建一个顺序读的对象，file中包含文件描述符和要操作的文件名
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192; // 1024*8
  char* space = new char[kBufferSize]; // 创建一个8k的缓存区域
  while (true) {
    Slice fragment; // 数据片段
    s = file->Read(kBufferSize, &fragment, space); // 将CURRENT中的内容读到 fragment 中
    if (!s.ok()) {
      break;
    } // 将数据片段读到 data 中，fragment 是一个指针，每次读取的数据都放到指向的位置
    data->append(fragment.data(), fragment.size()); // 这里发生拷贝
    if (fragment.empty()) { // 数据没读完继续读
      break;
    }
  }
  delete[] space; // 释放分配的空间
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {}

}  // namespace leveldb
