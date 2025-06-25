// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cstddef>
#include <vector>

#include "util/coding.h"

#include "gtest/gtest.h"
#include "hash.h"
namespace leveldb {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest : public testing::Test {
 public:
  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

    static constexpr int kCacheSize = 1000;
//  static constexpr int kCacheSize = 32;
  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  Cache* cache_;

  // 将当前对象的指针（this）赋值给类内的一个静态指针成员变量current_。
  // 这里的this是一个指针，指向正在执行该行代码的对象实例。

  // 这种做法常见于需要在静态成员函数中访问或修改非静态成员变量的情景。
  // 因为静态成员函数不与类的任何实例关联，它无法直接访问非静态成员。
  // 通过将this指针保存在静态成员变量中，可以在静态函数内部间接访问到特定实例的非静态成员。

  // 在上面提到的CacheTest类的例子中，静态成员函数Deleter需要访问CacheTest类的
  // 实例变量deleted_keys_和deleted_values_来记录被删除的键值对。
  // 通过事先在构造函数中将this指针赋值给current_，Deleter就能通过
  // current_->deleted_keys_和current_->deleted_values_来访问并修改这些成员变量了。

  // 当执行 current_ = this;
  // 这行代码时，可以确信执行该操作的实例已经被构造（至少是部分构造）。
  // 这是因为：this指针是在对象被构造时由编译器自动提供的，它指向正在构造的对象实例。
  // 只有当对象的构造过程已经开始，this指针才有效并可以被使用。
  // 在类的构造函数体内部执行此操作，意味着构造函数已经被调用，对象的构造过程正在进行中。
  // 此时，虽然可能还有一些成员变量尚未初始化，但对象的内存已经分配，且构造函数的执行上下文是存在的。
  // 因此，当赋值语句 current_ = this; 执行时，可以认为实例已经被构造到足以让
  // this指针可用的程度，但整个构造过程可能还没有完全结束，
  // 特别是如果这个赋值操作发生在构造函数的较早阶段。不过，无论如何，
  // this指针的存在确实表明了一个实例的构造过程已经在进行中
  CacheTest() : cache_(NewLRUCache(kCacheSize)) { current_ = this; }

  ~CacheTest() { delete cache_; }

  int Lookup(int key) {
    Cache::Handle* handle = cache_->Lookup(EncodeKey(key));
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache_->Value(handle));
    if (handle != nullptr) {
      cache_->Release(handle);
    }
    return r;
  }

  void Insert(int key, int value, int charge = 1) {
    cache_->Release(cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                                   &CacheTest::Deleter));
  }

  void StructPrint(int s) const { cache_->StructPrint(s); }

  Cache::Handle* InsertAndReturnHandle(int key, int value, int charge = 1) {
    return cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                          &CacheTest::Deleter);
  }

  void Erase(int key) { cache_->Erase(EncodeKey(key)); }
  static CacheTest* current_;
};
CacheTest* CacheTest::current_;

TEST_F(CacheTest, Encode) {
  std::cout << EncodeKey(100) << std::endl;
  std::cout << EncodeKey(200) << std::endl;
  std::cout << DecodeKey(Slice(EncodeKey(100))) << std::endl;
}

// offsetof(type,member)
void GetStructOffset() {
  struct LRUHandleMini {
    void* value;
    LRUHandleMini* next_hash;
    char key_data[1];
    // 上面为0时,sizeof(LRUHandleMini)=16
    // 上面为1时,sizeof(LRUHandleMini)=24
    // 说明柔性数组有初始值,会影响结构体大小,不然不占用存储空间
  };
  std::cout << sizeof(LRUHandleMini) << std::endl;
  std::cout << "value: " << offsetof(LRUHandleMini, value) << std::endl;
  std::cout << "next_hash: " << offsetof(LRUHandleMini, next_hash) << std::endl;
  std::cout << "key_data: " << offsetof(LRUHandleMini, key_data) << std::endl;
}

TEST_F(CacheTest, GetStructOffset) { GetStructOffset(); }

void GetTableIndex(int key) {
  std::string s = EncodeKey(key);
  uint32_t hash = Hash(s.data(), s.size(), 0);
  uint32_t shard_index = hash >> (32 - 4);
  uint32_t list_index = hash & (4 - 1);
  std::cout << "key: " << key << " hash: " << hash << " " << shard_index << " "
            << list_index << std::endl;
}

/*   -> shard_index  list_index
 * 0 ->  1,0
 * 9 ->  1,1
 * 49 -> 1,3
 * 58 -> 1,0
 * 67 -> 1,1
 * 76 -> 1,2
 * 116 ->1,0
 * 22 -> 2,0
 * 31 -> 2,1
 * */
TEST_F(CacheTest, GetTableIndex) {
  //  GetTableIndex(0);
  //  GetTableIndex(9);
  //  GetTableIndex(49);
  //  GetTableIndex(58);
  //  GetTableIndex(67);
  //  GetTableIndex(76);
  for (int i = 0; i < 500; i++) {
    GetTableIndex(i);
  }
}

TEST_F(CacheTest, HashZero) {
  // key: 0 hash: 405037440 1 0
  // key: 58 hash: 426928512 1 0
  // key: 116 hash: 448819840 1 0
  // key: 174 hash: 470696832 1 0
  // key: 232 hash: 492587904 1 0
  // key: 290 hash: 514478208 1 0
  // key: 317 hash: 302508204 1 0
  // key: 348 hash: 536369792 1 0
  // key: 375 hash: 324395692 1 0
  // key: 433 hash: 346291684 1 0
  // key: 491 hash: 368178532 1 0

  std::cout << "开始插入数据" << std::endl;
  Insert(0, 1);
  Insert(58, 2);
  Insert(174, 3);
  Insert(116, 4);
  Insert(232, 5);
  Insert(290, 6);
  Insert(317, 7);
  StructPrint(1);
  Insert(348, 8);
  Insert(375, 9);
  Insert(433, 10);
  Insert(491, 11);
  StructPrint(1);


  std::cout << "插入数据完成" << std::endl;
}

TEST_F(CacheTest, HashOne) {
  std::cout << "开始插入数据" << std::endl;
  Insert(0, 1);    // 405037440 0
  Insert(58, 2);   // 426928512 0
  Insert(9, 3);    // 334384673 1
  Insert(67, 4);   // 356270113 1
  Insert(116, 5);  // 448819840 0
  Insert(49, 6);   // 497586543  7   这里重新扩容，hash 函数已变
  Insert(76, 7);   // 285616206 6

  std::cout << "插入数据完成" << std::endl;
}

TEST_F(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
}

TEST_F(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0, deleted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
}

TEST_F(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0, deleted_keys_.size());

  cache_->Release(h1);
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, deleted_keys_.size());

  cache_->Release(h2);
  ASSERT_EQ(2, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[1]);
  ASSERT_EQ(102, deleted_values_[1]);
}

TEST_F(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);
  Insert(300, 301);
  Cache::Handle* h = cache_->Lookup(EncodeKey(300));

  // Frequently used entry must be kept around,
  // as must things that are still in use.
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000 + i, 2000 + i);
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(301, Lookup(300));
  cache_->Release(h);
}

TEST_F(CacheTest, UseExceedsCacheSize) {
  // Overfill the cache, keeping handles on all inserted entries.
  std::vector<Cache::Handle*> h;
  for (int i = 0; i < kCacheSize + 100; i++) {
    h.push_back(InsertAndReturnHandle(1000 + i, 2000 + i));
  }

  // Check that all the entries can be found in the cache.
  for (int i = 0; i < h.size(); i++) {
    ASSERT_EQ(2000 + i, Lookup(1000 + i));
  }

  for (int i = 0; i < h.size(); i++) {
    cache_->Release(h[i]);
  }
}

TEST_F(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2 * kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000 + i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}

TEST_F(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}

TEST_F(CacheTest, Prune) {
  Insert(1, 100);
  Insert(2, 200);

  Cache::Handle* handle = cache_->Lookup(EncodeKey(1));
  ASSERT_TRUE(handle);
  cache_->Prune();
  cache_->Release(handle);

  ASSERT_EQ(100, Lookup(1));
  ASSERT_EQ(-1, Lookup(2));
}

TEST_F(CacheTest, ZeroSizeCache) {
  delete cache_;
  cache_ = NewLRUCache(0);

  Insert(1, 100);
  ASSERT_EQ(-1, Lookup(1));
}

}  // namespace leveldb
