// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include <cassert>
#include <cstdio>

#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname);
// 制作文件名
static std::string MakeFileName(const std::string& dbname, uint64_t number,
                                const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

std::string LogFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "log");
}
// 创建leveldb 数据文件
std::string TableFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "ldb");
}

std::string SSTTableFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "sst");
}
// 创建以 MANIFEST 文件名
std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
                static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG.old";
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type) {
  Slice rest(filename);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest; // 剩下的是后缀名
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}
// 先创建 .dbtmp 的临时文件，写入当前的 MANIFEST,再改名为 CURRENT
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  // 根据传入的编号获取指定的 MANIFEST 文件名
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"get manifest filename {}",manifest);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  // 创建一个临时文件 dbname/000001.dbtmp
  std::string tmp = TempFileName(dbname, descriptor_number);
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"create tmp file {}",tmp);
  // 去掉 dbname 后的新建的文件名 MANIFEST 写到临时文件中
  // 文件不存在会新建文件
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    // 将刚新建的临时文件改名
    // 例如 000001.dbtmp ->CURRENT 里面存的数据为 MANIFEST-000001,即 MANIFEST 文件名
    // MANIFEST 存的 log 文件,即 edit 相关信息,即每一层 table 的信息
    s = env->RenameFile(tmp, CurrentFileName(dbname)); // 000001.dbtmp 改名为 CURRENT
  }
  if (!s.ok()) {
    env->RemoveFile(tmp);
  }
  return s;
}

}  // namespace leveldb
