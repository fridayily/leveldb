// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "leveldb/spd_logger.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
//  entries 都有一个 in_cache 的布尔型变量标识这个 entry 是否被引用
//  要改变这个标识为 false, 除了通过 Erase()调用 deleter 方法外,唯一的方法是调用
//  Insert() 当传入的是重复 key ,或者调用 cache 的析构函数
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

/*
 * cache 中维护了两个链表, 一个 结点 要么属于其中一个,要没都不属于
 * 仍然被客户端引用但从缓存中删除的项不在这两个列表中
 *  in-use : 包含当前被客户端引用的 条目,没有特别的顺序
 *         : 此链表用于不变检查
 *
 *  LRU : 保存客户端当前未引用的项
 *  在LRU顺序中，当它们检测到缓存中的元素获取或丢失其唯一的外部引用时，
 *  元素通过Ref()和Unref()方法在这些列表之间移动。
 *
 *
 * */

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
/*
 * 一个 entry 可变长度的在堆中分配的结构体  sizeof(LRUHandle) = 72
 * 多个 entry 依据访问时间构成一个循环双向链表
 * */
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;  // Whether entry is in the cache.
  uint32_t refs;  // References, including cache reference, if present.
  uint32_t hash;  // Hash of key(); used for fast sharding and comparisons
  char key_data
      [1];  // Beginning of key  柔性数组,[0] [1]
            // 相同效果,适合制作动态数组,直接把结构体和缓冲区数据放在一起,这样便于分配和释放内存

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

/*
 * 我们提供了自己的简单哈希表，因为它消除了一大堆移植操作，
 * 而且在我们测试过的一些编译器和运行时组合中，它比一些内置哈希表实现要快。
 * */
// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) {
    /* 初始化构造函数是进行一次hash表的初始化, 即 LRUHandle*[4] */
    Resize();
  }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);  // 返回一个 LRUHandle
  }

  LRUHandle* Insert(LRUHandle* h) {
    // (1)list 为空 返回的是空
    // (2) list 有数据，find时没有出现hash冲突，则返回最后一个结点的 next_hash
    // 的地址 (3) list 有数据，中间有hash 冲突，则返回冲突的那个结点的地址
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;  // ptr 为空说明没找到
    // 第一次插入时, old 指向空,h->next_hash 指向空
    // 如果找得到,替换老结点
    /*
     * 下面2行就是最基础的向单向链表中添加一个结点的代码
     * 如果old 不为空,说明找到 hash
     * 相同的结点,那么用新结点的指向替换掉旧结点的指向
     *  (1)如果链表为 A->B->C->D
     * 现在要插入结点E，返回D->next_hash结点的地址，即NULL,先将E指向D->next_hash,然后D指向E，最后形成
     * A->B->C->D->E (2)如果链表为 A->B->C->D 现在要插入结点C(h
     * 新结点)，返回C(old hash冲突结点)结点地址，则h->next_hash 指向D,*ptr
     * 替换为C(h),即新C替换为旧C 这个 h 就是刚存入 in_use_ 的结点
     * */
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;  // 将FindPointer 返回的地址指向向要插入的 LRUHandle 对象
               // 即表里存入一个结点
    if (old == nullptr) {      // old 为空,说明插入的key不在 hash
                               // 表里,要新增key,就可能需要扩容
      ++elems_;                // 加入一个 LRUHandle 元素后,元素数量加 1
      if (elems_ > length_) {  // 当存入当元素大于 length_(默认4) 时扩容
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  /*
   * 返回要移除到结点
   * */
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr =
        FindPointer(key, hash);  // 返回的是前驱结点的 next_hash 的地址
    LRUHandle* result = *ptr;    // next_hash 里面保存的地址
    if (result != nullptr) {
      // int   i=10;
      // int*  a=&i; -> result(*ptr)
      // int** b=&a; -> ptr
      // 原来 *ptr 保存的是所要查找 key 的结点,现在指向key的下一个结点
      *ptr = result->next_hash;  // 将 next_hash 保存的地址更新为下一个结点地址
      /*即删除result结点，将 result 空出来*/
      --elems_;
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Remove {}", result->value);

    return result;  // 返回删除的结点
  }

  /* 辅助代码 */
  void ListPrint() const {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "list begin");
    for (int i = 0; i < length_; ++i) {
      LRUHandle* cur = list_[i];
      if (cur == nullptr) {
        continue;
      }
      printf("hash %d ", i);
      do {
        printf("%p ", cur->value);
        cur = cur->next_hash;
      } while (cur != nullptr);
      printf("\n");
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "list end");
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;
  // 一个由 LRUHandle* 组成的数组，即每个数组元素都是指针，指向 LRUHandle
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    // 先用 hash 值的 length_-1 位确定 key 存在哪个 list_ 中
    //    printf("FindPointer hash %d list_[i] %d  %d\n", hash, length_,
    //           hash & (length_ - 1));
    // list_ 是一个数组,里面存的LRUHandle*,对之&,就是 LRUHandle**
    LRUHandle** ptr = &list_[hash & (length_ - 1)];

    // ptr 指向的是一个 LRUHandle 链表 ，初始时，该链表为空，
    // 遍历 LRUHandle 对象的 hash 值与传入的hash值相比, 直到相等为止
    // 或者 key 相等
    // 然后返回这个对象的地址
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      // 这里 &优先级最低
      // ptr 指向一个数组中元素的地址
      // *ptr 指向指针一个 LRUHandle 对象
      // (*ptr)->next_hash 指向的是下一个 LRUHandle 对象,
      // &(*ptr)->next_hash;  取出的是保存 next_hash 变量的地址
      ptr = &(*ptr)->next_hash;
      // 返回的是所查找结点
      // 指针,要删除就修改这个指针的指向
      //
    }
    return ptr;
  }

  void Resize() {
    //    printf("Resize() 被调用\n");
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;  // 扩容2倍
    }
    LRUHandle** new_list =
        new LRUHandle*[new_length];  // 新建数组,容量是以前2倍,里面存的是
                                     // LRUHandle 型的指针
    memset(new_list, 0, sizeof(new_list[0]) * new_length);  // 将数组初始化为0
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {  // 这里length_ 还是扩容前的length
      LRUHandle* h = list_[i];                // 将list_ 的首地址放到新数组  h
                                // 存的一个指针,指向一个 LRUHandle 对象
      while (h != nullptr) {
        // 遍历list[i]中的单向链表(next_hash指针组成),将里面所有元素重新
        //  hash 后放入新的数组中
        // 备份下一个要遍历的结点
        LRUHandle* next = h->next_hash;
        // next_hash 存的是hash值相同的下一个结点的地址
        // 下面会将每个结点重新hash 到扩容后的链表数组中
        uint32_t hash = h->hash;  // 获取结点 hash 值，每必要重新计算
        // 这里返回的是新建数组的首地址
        // ptr 是new_list[i] 那个单元格的地址,而不是单元格里面存储的地址
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];

        //  对 h 而言,next_hash 构成单向链表
        //  例如 list_ 一起指向一个双向链表
        //  其中 list_[0] 是由 next_hash 组层的单向链表
        // 这是最普通的单向链表的插入值的方法    ptr 是list[i]的地址，*ptr
        // 指向一个LRUHandle 对象
        h->next_hash = *ptr;  // h.next_hash 保存 new_list[i] 的地址
                              //  ptr 指向链表的第一个结点
        *ptr = h;             // new_list[i] 保存 h 结点的地址,即现在 list[i]
                              // 现在保存的是重新hash后的结点
        // 上面两行是将一个重新hash 后的结点插入到对应链表头部

        h = next;  // 相同 hash 结点也组成一个链表,这里处理下一个结点
        count++;
      }
    }
    assert(
        elems_ ==
        count);  // 确保所有的元素都重新 hash
                 // 了一次,在这个过程中,整个双向链表还是原来的双向链表,只是各结点重新hash到新的
                 // list
    delete[] list_;  // 释放旧的表
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

  /* 辅助代码*/
  void StructPrint() const {
    MutexLock l(&mutex_);
    LRUHandle* cur_lru = lru_.next;
    LRUHandle* cur_in_use = in_use_.next;
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "begin ");

    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "lru_");
    while (cur_lru != &lru_) {
      printf("%p ", cur_lru->value);
      cur_lru = cur_lru->next;
    }
    printf("\n");

    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "in_user_");
    while (cur_in_use != &in_use_) {
      printf("%p ", cur_in_use->value);
      cur_in_use = cur_in_use->next;
    }
    printf("\n");
    table_.ListPrint();
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "end ");
  }

 private:
  void LRU_Remove(LRUHandle* e);  // 将结点 e 从当前双向链表中删除
  void LRU_Append(LRUHandle* list, LRUHandle* e);  // 将结点 e 追加到链表头
  void Ref(LRUHandle* e);  // 当引用计数为1 时,将 e 移动到 in_use_ 链表
  void Unref(
      LRUHandle* e);  // 减少 e 的引用计数,为0时销毁,为1时,将其移动 lru_ 链表
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);  // 当前已使用结点数量

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  /*
   * lru 双向链表的空表头
   * lru.prev 指向最新的 entry, lru.next 指向最旧的 entry
   * 每个 entry refs==1 和 in_cache==true
   * */
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  /*
   * in-use 双向链表的空表头
   * 该链表中 refs>=2 且 in_cache==ture
   * */
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);
};
// 初始化 LRUCache, 将 lru_，in_use_ 的 next,prev 指向自己
LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  //  printf("LRUCache......%d\n", __LINE__);
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  //  printf(" ~LRUCache 调用\n");
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

/* 将结点添加到 in_use 链表 */
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);  // 如果在 lru_ 链表中,先删除该结点,然后将其移动到 in_use_ 中
    LRU_Append(&in_use_, e);  // 将结点添加到 in_use_ 中
  }
  e->refs++;  // 引用计数加1
}

/* 将结点添加到 lru_链表 或者将其销毁 */
void LRUCache::Unref(LRUHandle* e) {
  //  printf("LRUCache 函数被调用\n");
  assert(e->refs > 0);
  e->refs--;           // 引用个数减1
  if (e->refs == 0) {  // Deallocate.  引用为0时,释放分配的空间
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);  // 调用删除函数
    free(e);                            // 释放new 创建的结点
  } else if (e->in_cache &&             // 结点要么在 in_cache
                                        // 要么在lru_中，满足这个条件驱逐
             e->refs == 1) {            // 插入的结点还在 cache 中且还有一个引用
    // No longer in use; move to lru_ list.
    LRU_Remove(e);         // 不在使用,移出 in_use
    LRU_Append(&lru_, e);  // 将新结点添加到 lru
  }
}
// 在双向链表中删除 e 结点
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;  // 在循环双向链表中删除一个结点
  e->prev->next = e->next;
}

/*
 * 总是把结点添加到链表的尾部,这样list->next 总是指向最新的结点
 * */
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  // 指向 LRUCache 结构体中的 lru_ 或者 in_use_ ,相当于空的头结点
  e->next = list;        // 将新结点next指向头结点
  e->prev = list->prev;  // 新结点prev 指向之前的最后一个结点
  e->prev->next = e;     // 之前最后一个结点的下一个结点替换为 e
  e->next->prev = e;     // 头结点的prev 指向新加入结点
}

/* 有查找操作,就可能将结点添加到 in_use_ 链表 */
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    /* 调用 lookup 成功 引用加1 */
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

/* 释放操作将结点添加到 lru_ 链表 或者释放结点  */
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);
  // 创建一个 LRUHandle 结点，然后初始化里面每个元素
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  // 类似柔性数组的分配方式 ,key_data 保存key, key_data[1]  sizeof(LRUHandle)=72
  // 分配1个字节,或者根据结构体内存对齐来确定占用多少空间
  e->value = value;            // 指针指向存入的值
  e->deleter = deleter;        // 绑定 delete 函数
  e->charge = charge;          // 绑定
  e->key_length = key.size();  // key 的长度
  e->hash = hash;              // hash 值
  e->in_cache = false;
  e->refs = 1;  // for the returned handle. 引用计数
  std::memcpy(e->key_data, key.data(), key.size());  // 保存 key 的第一个字节

  if (capacity_ > 0) {
    e->refs++;           // for the cache's reference.
    e->in_cache = true;  // 有可用容量才放入 cache
    //    printf("&in_use_ %x,refs %d\n", &in_use_, in_use_.refs);
    /* 添加到in_use_ 链表*/
    LRU_Append(&in_use_, e);
    // in_use_存的不是指针,是一个类似int/long的实例,占72个字节
    usage_ += charge;  // 修改shard_ 数组中一个 LRUCache 的使用数量
    FinishErase(
        table_.Insert(e));  // insert 返回不为空,说明有hash 冲突的键,要删除它
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;  // 没有容量不放入cache
  }

  // 当数据超出容量时，将不被客户端引用的数据条目驱逐出内存
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  /*
   * 如果 e 不为空,说明 插入了 hash 冲突的键, e 是旧键,需要释放
   * */
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);        // 从双向链表中删除结点
    e->in_cache = false;  // 将 in_cache 置为 false
    usage_ -= e->charge;
    Unref(e);  // 删除结点或者移动到 lru_
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
// static const int kNumShardBits = 1;
static const int kNumShards = 1 << kNumShardBits;
// 先初始化父类成员变量，再初始化父类构造函数，再初始化子类的成员函数，最后子类构造函数
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_
      [kNumShards];  // 一个数组包含 16个 LRUCache
                     // 对象,初始化ShardedLRUCache时，会创建16个LRUCache对象
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }  // hash 长度是32位,向右移32-4 位,还剩4位,用于确定选取哪个 LRUCache 对象

 public:
  // 子类声明了构造函数，创建子类对象时，先调用父类的默认构造函数，再调用子类的构造函数
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "ShardedLRUCache construct");
    const size_t per_shard =
        (capacity + (kNumShards - 1)) /
        kNumShards;  // (32+ 15)/16 = 2,即 每个 shard_ 分配2个
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);  // 将要分配的总空间分成16份
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);  // 在shard_[16] 中查找
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }

  /* 自己添加的辅助代码,打印第 s 个 shard 的 in_user lru*/
  void StructPrint(int s) const override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "the s {} th shard", s);
    shard_[s].StructPrint();
    //    ListPrint();
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create NewLRUCache");
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
