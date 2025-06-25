// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

class LEVELDB_EXPORT Cache {
 public:
  Cache() = default;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle {}; // 可以作为抽象的基类

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  // 插入键值对到 cache 中,并从缓存容量中减去该键值对所占额度
  //
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // 返回对应的键值对的 handle, 当返回的 mapping 不在需要时,需要调用
  // this->Release(handle)
  // 释放
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  //
  // 当插入的 entry 不再需要时, key 和 value 会传入 deleter
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  // 如果 cache 中没有匹配的 key,返回 nullptr
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // 否则返回对应键值对的 handle
  // 当返回的 mapping 不再使用时,要调用 this->Release(handle) 释放
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // 释放上面 LookUp 返回的句柄
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // 获取 Lookup 返回的句柄中的value
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
   // 句柄必须是同一实例返回
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  // 如果缓存中包含指定键所指向的条目,删除它
  //
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  // 返回一个新的数值id, 当一个缓存实例由多个客户端共享时,为了避免多个客户端的键冲突
  // 每个客户端可能想获取一个独有的id,并将其作为键的前缀
  // 类似于给每个客户端一个单独的命名空间
  virtual uint64_t NewId() = 0;

  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  // 驱逐全部没有被使用的数据条目
  // 内存吃紧型的应用可能想利用此接口定期释放内存
  // 基类中的 Prune 默认实现为空,单强烈建议所有子类自行实现
  // 将来的版本可能会增加一个默认实现
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  // 返回当前缓存中所有数据条目所占总容量和的一个预估
  virtual size_t TotalCharge() const = 0;



  virtual void StructPrint(int s) const =0;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
