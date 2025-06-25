// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
#define STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_

#include <string>

#include "leveldb/export.h"

namespace leveldb {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since leveldb may invoke its methods concurrently
// from multiple threads.
class LEVELDB_EXPORT Comparator {
 public:
  virtual ~Comparator();

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  // 当实现的比较器导致任意两个key的相对顺序改变时，使用者应该换一个新名称
  // Names starting with "leveldb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // 简单比较器实现可能直接返回未改变的 *start
  // 对这个方法的实现可以是空的
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // 改变*key为一个 >= *key 的短字符串
  // 简单比较器实现可能直接返回未改变的 *key
  virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Return a builtin comparator that uses lexicographic byte-wise 返回一个内置比较器，它使用按字典顺序按字节排序
// ordering.  The result remains the property of this module and
// must not be deleted.  // 结果仍然是该模块的属性，不能删除

// 链接器可见性：LEVELDB_EXPORT 是一个预处理器宏，用于控制函数在链接时的可见性。
// 将 BytewiseComparator 定义在类外部，可以更方便地控制这个宏的使用，
// 确保在编译库时正确地导出或导入这个函数，以便其他模块可以使用。

// 静态成员的实现：虽然 BytewiseComparator 不是静态成员函数，
// 但它可能在内部使用了静态成员变量来实现单例模式。将这样的函数定义在类外，
// 可以更好地管理静态成员的生命周期和访问权限，因为静态成员变量通常不在类的接口中暴露。
LEVELDB_EXPORT const Comparator* BytewiseComparator();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
