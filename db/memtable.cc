// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {
 // 传入的是 value 的起始地址时，返回value 或者
 // 传入的是 internal key= key_length(varint) + key
 // + value_type + seq + val_length(varint) + val
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data; // 地址的开头存的是 value 的长度
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted 取出长度
  // p 是    len 是 internal_key 的长度，p 中现在还包含 internal_key + val_len + val
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator) // 初始化 skip_list
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); } // 返回key
  Slice value() const override { // 返回value
    Slice key_slice = GetLengthPrefixedSlice(iter_.key()); // 从 memKey 提取 InternalKey
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size()); // key+ key_length ,后面是数据
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }
// 将 k,v 封装成 memkey 存到skip_list
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8; // 7 字节的 sequence number + 1 字节的 type
  // 一个单元的结构是 key_size + key + seq + type + value_size + value
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len); // 在内存池中分配指定长度的区域
  char* p = EncodeVarint32(buf, internal_key_size); // 将internal_key_size 编码后存入buf
  std::memcpy(p, key.data(), key_size); // key 存入buf
  p += key_size; // 移动指针
  EncodeFixed64(p, (s << 8) | type); // 将seq 和 type 存入 buf
  p += 8; // 移动指针
  p = EncodeVarint32(p, val_size); // 存入 val_size ,val_sizer 为0 也会占用1字节空间
  std::memcpy(p, value.data(), val_size); // 存入data
  assert(p + val_size == buf + encoded_len);  // 判断实际存入的长度和理论存入长度是否相同
  table_.Insert(buf); // 存入 table 中
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key(); // 就是完整的 internal_key_length + internal_key
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data()); // 定位到大于等于 memkey 的第一个key
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]47/
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    // 根据memkey 从skip list里获得到大于等于指定key的entry，里面有k,v
    const char* entry = iter.key();
    uint32_t key_length; // 用于记录 Internal Key 长度
    // 指向 user_key 开头，保存 key_length
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    //迭代器里面取出来的值和要查找的key 比较，这里版本号可能不一样
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key  说明 user_key 相等
      // 取出 sequence num + valueType，刚好 8 字节
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          // 传入的是value的开始地址,取得 v
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size()); // value 重新赋值
          return true;
        }
        case kTypeDeletion:
          // 查找返回的entry 类型是 del 的，则直接设置找不到
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}


// std::string ParseMemTableKey(const char* key) {
//   uint32_t key_length;
//   const char* key_ptr = GetVarint32Ptr(key, key + 5, &key_length);
//
//   Slice internal_key(key_ptr, key_length);
//   ParsedInternalKey parsed_key;
//
//   if (ParseInternalKey(internal_key, &parsed_key)) {
//     return parsed_key.DebugString();
//   } else {
//     return "[Invalid key]";
//   }
// }
//
// // 打印MemTable中的SkipList结构
// void PrintMemTableSkipList(const MemTable& memtable) {
//   // 获取内部的SkipList
//   const auto& table = memtable.GetTable();
//
//   // Step 1: 收集所有键及其原始指针
//   struct KeyInfo {
//     const char* raw_key;
//     std::string parsed_key;
//   };
//   std::vector<KeyInfo> all_keys;
//
//   {
//     typename MemTable::Table::Iterator iter(&table);
//     for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
//       const char* raw_key = iter.key();
//       all_keys.push_back({raw_key, ParseMemTableKey(raw_key)});
//     }
//   }
//
//   if (all_keys.empty()) {
//     std::cout << "SkipList is empty." << std::endl;
//     return;
//   }
//
//   // Step 2: 获取SkipList的最大高度
//   int max_height = table.GetMaxHeight();
//
//   // Step 3: 记录每一层的节点
//   std::vector<std::unordered_set<const char*>> level_nodes(max_height);
//
//   // 注意：这里需要访问SkipList的私有成员head_
//   // 假设PrintSkipList函数是SkipList的友元
//   auto head = table.head_;
//
//   for (int level = 0; level < max_height; ++level) {
//     auto current = head;
//     while (true) {
//       auto next = current->Next(level);
//       if (next == nullptr) break;
//       level_nodes[level].insert(next->key);
//       current = next;
//     }
//   }
//
//   // Step 4: 打印SkipList结构
//   std::cout << "MemTable SkipList structure:" << std::endl;
//   std::cout << "Max height: " << max_height << std::endl;
//   std::cout << "-------------------------------------------------------------" << std::endl;
//
//   // Step 5: 从上到下打印每一层
//   for (int level = max_height - 1; level >= 0; --level) {
//     std::cout << "Level " << std::setw(2) << level << ": ";
//
//     // 打印HEAD
//     std::cout << "HEAD -> ";
//
//     // 打印当前层的节点
//     for (size_t i = 0; i < all_keys.size(); ++i) {
//       if (level_nodes[level].count(all_keys[i].raw_key)) {
//         // 节点在当前层
//         std::cout << all_keys[i].parsed_key;
//       } else {
//         // 节点不在当前层，打印空格
//         // 估算需要的空格数
//         size_t spaces_needed = all_keys[i].parsed_key.size();
//         std::cout << std::string(spaces_needed, ' ');
//       }
//
//       // 打印箭头（除了最后一个节点）
//       if (i < all_keys.size() - 1) {
//         std::cout << " -> ";
//       }
//     }
//
//     // 打印层结束
//     std::cout << " -> nullptr" << std::endl;
//   }
//
//   std::cout << "-------------------------------------------------------------" << std::endl;
// }
}  // namespace leveldb
