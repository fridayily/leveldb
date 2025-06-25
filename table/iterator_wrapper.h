// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {
// 缓存底层迭代器的valid()和key()方法的结果。
// 这样做的好处是可以减少虚拟函数调用，并且提高缓存局部性，从而优化性能
// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
  }

  // Iterator interface methods
  bool Valid() const { return valid_; }
  Slice key() const {
    assert(Valid());
    return key_;
  }
  Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const {
    assert(iter_);
    return iter_->status();
  }
  void Next() {
    assert(iter_);
    iter_->Next();
    Update();
  }
  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  void SeekToFirst() {
    assert(iter_);
    iter_->SeekToFirst(); // 将key,value 存到 iter的私有变量
    Update();
  }
  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
    Update(); //
  }

 private:
  void Update() {
    valid_ = iter_->Valid(); // 没有遍历结束就返回true
    if (valid_) {
      key_ = iter_->key(); // 将iter_的Key 赋值给 key_
    }
  }

  Iterator* iter_;
  bool valid_;
  Slice key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
