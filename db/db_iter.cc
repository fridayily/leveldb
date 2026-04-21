// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"

#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s, uint32_t seed)
      : db_(db),
        user_comparator_(cmp),  // user_key 比较器
        iter_(iter),            // 迭代器
        sequence_(s),           // 版本号
        direction_(kForward),   // 迭代方向
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;
  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) { dst->assign(k.data(), k.size()); }
  // std::capacity 当前字符串对象可容纳的最大字符数，而不进行重新分配
  // 当 saved_value_ 达到指定的阈值后，用swap 清空，而非 clear
  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      //  使用 swap() 而不是直接调用 clear() 方法是为了避免在 saved_value_
      //  容量特别大时造成不必要的内存管理开销，通过交换操作能更高效地释放原有数据占用的空间。

      // clear
      // 会将字符串长度设为0，并不一定立即释放已分配的内存空间，它通常保持当前容量不变，以便后序追加操作时能够
      // 重用这块内存，从而避免频繁申请和释放内存带来的开销
      // empty 本身没有分配内存，交换后 saved_value_ 的内存空间被释放
      // 交换操作执行的时原子性的状态变更，相比于 clear() 可能触发的内存收缩操作，性能更优

      // clear 尽管现有的实现在恒定时间内运行，但与字符串的大小呈线性关系
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() { return rnd_.Uniform(2 * config::kReadBytesPeriod); }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  // 初始化时该变量被设置的随机值，每次迭代器读取时扣减该值
  // 耗尽时 用RecordReadSample 检查键的重叠情况，可能触发压缩
  size_t bytes_until_read_sampling_;
};
/*
 * 将迭代器中的key(internal_key)解析到 ParsedInternalKey 中
 *
 * 通过读取采样机制，自动触发压缩操作
 * 保持数据库的良好状态，提高查询性能
 *
 * note: 如果数据库中没有重复键，ParseKey 不会触发压缩
 *      VersionSet::PickCompaction() 会根据压缩分数或者查询次数来触发压缩
 */
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();  // 获取 internal_key

  // 获取 key(internal_key) +value长度，即要读取的 key 和 value 的字节数
  size_t bytes_read = k.size() + iter_->value().size();
  // 如果剩余能扣除的字节数小于需要减去的字节数（待读取字节数），则重新设置
  // bytes_until_read_sampling_ 添加一个随机值，并调用 RecordReadSample 进入 while 循环可能会触发
  // Compaction
  //  如果生成的 bytes_until_read_sampling_ 较小，重新增加RandomCompactionPeriod()
  // 这里可能多次 Compaction
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);  // 可能触发 Compaction
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  // 每次迭代器读取，都会从 bytes_until_read_sampling_中减去 bytes_read
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

/*
 *
 * 定位到下一个有效的 key
 *
 * 如果键是,
 *    [a,b,c]
 *   1. kForward iter_.key=a, saved_key_=a, iter_.key=b
 *   2. kReverse iter_.key=b, saved_key_=a, iter_.key=a
 */
void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // note: saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    // 如果是一连串的 key, 则跳过这些 key
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

/*
 *  注意：这里的迭代器是 skip_list,key 都是有序的
 *
 *  找到下一个有效的key
 *  即跳过重复的，删除的key
 *
 *  假设1
 *  put(key1) put(key1) del(key1)
 *     结果形如 key1_3_(kTypeDeletion)  key1_2_(kTypeValue)  key1_1_(kTypeValue)
 *     do...while{} 过程如下
 *     第一次读取出 key1_3,是标记为删除的，skipping = true
 *     下一次取出 key1_2,与*skip 的 user_key 相等, iter_->Next() 继续循环查找
 *     再一次取出 key1_1,与*skip 的 user_key 相等,iter_->Next() 继续循环查找
 *
 *  假设2
 *     [key1_2_kTypeValue][key2_1_kTypeValue]
 *     DBIter::Next()执行时 saved_key_ = key1, 迭代器指向 key2_1_kTypeValue
 *     do...while{} 第一次读取出 key2，key2>key1 ,跳出循环，迭代器还是指向 key2
 */
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    // ikey.sequence 是存入key 的时候的sequence
    // iter_ 中的 key 是 internal_key，这里查找第一个key 存在，
    // 且 key 的版本号小于等于 sequence_ 的迭代器，找到返回 true
    // sequence_ 是数据库读取是的版本号，如果查找的 key 的版本号小于等于
    // sequence_，返回true，可以继续判断
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);  // 如果查找的key是被删除的，保存到skip中
          skipping = true;
          break;
        case kTypeValue:
          if (skipping && user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            // 当查找的新的 user_key 时返回
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
    // 只要迭代器有效就一直循环查找
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

/*
 * 假设键为 [<a,1>,<b,1>,<b,2>,<b,3>,<c,3>]
 * 迭代器指向c,现在调用 Prev() 找前一个键
 *    会依次保存 <b,3> <b,2> <b,1> 到 <saved_key_,saved_value_>
 *    然后迭代器指向 <a,1>
 * 注意:
 *   DBIter::key() 会根据迭代方向的不同返回不同的值
 *     (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
 */
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          // 当 saved_value_ 容量过大时，使用 swap 操作释放内存,避免内存占用过高
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}
// 找到第一个能读取的key, 因为设置了版本号，迭代器可能无效
void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    // 迭代器指向结点不为空即有效
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator, Iterator* internal_iter,
                        SequenceNumber sequence, uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
