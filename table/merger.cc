// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
/*
 * MergingIterator 的迭代器数量与 LevelDB 的存储层次结构和当前数据库状态直接相关。
 * 它的子迭代器数量取决于需要合并的数据源数量，主要由以下因素决定：
 * 1. 内存表（MemTable）状态
 *   活跃 MemTable：如果存在活跃的 MemTable，会贡献 1 个子迭代器
 *   Immutable MemTable：如果存在待压缩的 Immutable MemTable，会贡献 1 个子迭代器
 * 2. Level 0 SST 文件数量
 *   Level 0 的 SST 文件可能相互重叠，因此每个文件都会贡献一个子迭代器
 *   Level 0 文件数量取决于：
 *      内存表压缩的频率
 *      写入负载的大小和模式
 *      Options.write_buffer_size 的设置（影响内存表大小，进而影响压缩频率）
 *   Level 1 及以上层级
 *      Level 1 及更高层级的 SST 文件是有序且不重叠的，因此每个层级只会贡献 1 个子迭代器
 *      这些层级的迭代器通常是 TwoLevelIterator，内部会处理该层级的所有文件
 *
 */
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"begin");
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  /*
   * 假设有两子迭代器：
   *   迭代器 A：包含键 [a, c, e]
   *   迭代器 B：包含键 [b, d, f]
   * SeekToFirst
   *   （1）定位到每个迭代器的第一个位置
   *   （2）调用 FindSmallest，找到最小键所属的迭代器，赋值给 current_, 此时 current_ 指向 A
   * 调用 Next():
   *    方向是 kForward，不需要调整其他子迭代器
   *    迭代器 A 前进到 "c"
   *    FindSmallest() 比较 "c"（A）和 "b"（B），找到最小键 "b"，current_ 指向迭代器 B
   * 再次调用 Next()：
   *    方向是 kForward，不需要调整其他子迭代器
   *    迭代器 B 前进到 "d"
   *    FindSmallest() 比较 "c"（A）和 "d"（B），找到最小键 "c"，current_ 指向迭代器 A
   * 依此类推，最终返回有序序列 [a, b, c, d, e, f]
   *
   *
   * 对于大量子迭代器的情况，可以考虑使用更高效的数据结构（如优先队列）来维护最小键，
   * 减少 FindSmallest() 的线性时间复杂度
   */
  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    // 使当前子迭代器前进到下一个键
    // 这里的 current_ 是 IteratorWrapper，而不是 version
    current_->Next();
    // 遍历所有子迭代器，找到新的最小键的子迭代器作为新的 current_
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace


Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(),"begin");
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
