// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    // 分配了新的空间,只分配了 bytes 大小,而不是 kBlockSize
    // 这个 result 被记录在 blocks_[i] 中
    // 在 block[i-1] 指向的内存中还有部分未使用的内存可以继续使用
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize); // 分配新块
  alloc_bytes_remaining_ = kBlockSize; // 新块的大小就是剩余可分配大小

  char* result = alloc_ptr_; // 指向刚开辟出来的 kBlockSize 空间
  alloc_ptr_ += bytes; // 分配 bytes 出去
  alloc_bytes_remaining_ -= bytes; // 可用空间减少
  return result;
}


char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8; // 判断指针长度
  // 如果align不是2的幂，那么表达式(align & (align - 1))的结果将不等于0，
  // 导致编译错误并显示错误信息
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");

  // 这个操作可以得到alloc_ptr_相对于对齐边界偏移了多少字节
  // 因为只有高地址最高位是1，其余都是0，才是对齐的，
  // align -1 对应的位数就是不对齐的字节数
  // 如果分配是对齐的，则 地址应该是 8 的倍数
  // 假设 32 位 CPU
  // 0x0:00000000 00000000
  // 0x8:00000011 00000000
  // alloc_ptr_ = 0x0b
  // 0x0b&0x7 = 3 = current_mod
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 8 - current_mod = 5 = slop
  // slop代表剩余的不对齐的空闲空间，即要补齐的字节数，以便下一个分配的位置是对齐的。
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // 跳过不对齐的地址 + 要分配的空间 就是实际需要分配的空间
  size_t needed = bytes + slop;
  char* result;
  // 剩余的空间足够使用
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop; // 调整后字节是对齐的
    alloc_ptr_ += needed; // 字节对齐后再分配内存
    alloc_bytes_remaining_ -= needed; // 减去刚分配出去的内存
  } else {
    // AllocateFallback always returned aligned memory
    // 说明剩余空间不够使用
    // 有两种情况
    // 其一:
    //     剩余 16字节,要分配 32 字节,重新开辟一个 4k 的空间
    // 其二:
    //      剩余 1K, 要分配 2K, 会开辟一个新的空间，返回新空间的地址
    // 但是 alloc_bytes_remaining_ 并没有设置为 0，alloc_ptr_ 指针也没有移动
    // 当下次要使用的内存小于 alloc_bytes_remaining_ 时可以继续使用，减少了内存的浪费
    // 这次分配产生的新地址会被 alloc_ptr 覆盖
    result = AllocateFallback(bytes);

  }
  // 对 64位操作系统，align -1 = 7= b111, 如果地址后面3位为0，说明是4字节对齐的
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes]; // 开辟新空间
  // 每次分配返回的指针存到一个 vector 中,用一个数组记录了每次分配的情况
  blocks_.push_back(result);
  // memory_usage_ 记录了所有分配内存大小,包括刚分配的地址
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}
} // namespace leveldb
