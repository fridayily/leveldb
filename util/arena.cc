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

/*
 * 此函数一般由 Arena::Allocate(size_t bytes) 调用
 * 设要分配的字节数为 n:
 * case1:
 *   alloc_bytes_remaining_ = 0
 *   分配一个 4096 字节的 block
 *   alloc_bytes_remaining_ = 4096
 *   从中获取 n 个字节使用，alloc_ptr_ 指向 &block + n
 * case2:
 *   n < alloc_bytes_remaining_
 *   从 alloc_bytes_remaining_ 扣减 n 个字节, alloc_ptr_ 指向 &block + n
 * case3:
 *   n > alloc_bytes_remaining_ && n > kBlockSize / 4
 *   直接分配一个大的内存块使用，alloc_ptr_ 保持不变，
 *   这样下次需要小内存时，还是可以使用上一个 block 的内存
 *  case4:
 *    分配一个新的 4096 的内存块
 *    alloc_bytes_remaining_ = 4096
 *    从中获取 n 个字节使用，alloc_ptr_ 指向 &block + n
 */
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    // 分配了新的空间,只分配了 bytes 大小,而不是 kBlockSize
    // 注意这里直接返回了 result
    // 假如这里是分配第 i 块 block[i]
    // alloc_ptr_ 还是指向 block[i-1] 中还未分配出去的内存的地址
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // 分配新块
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  // 新块的大小就是剩余可分配大小
  alloc_bytes_remaining_ = kBlockSize;
  // 指向刚开辟出来的 kBlockSize 空间
  char* result = alloc_ptr_;
  // 分配 bytes 出去
  alloc_ptr_ += bytes;
  // 可用空间减少
  alloc_bytes_remaining_ -= bytes;
  return result;
}


char* Arena::AllocateAligned(size_t bytes) {
  // 判断指针长度
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 如果 align 不是 2 的幂，那么表达式 (align & (align - 1)) 的结果将不等于 0，
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
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "allocate {} bytes",block_bytes);
  char* result = new char[block_bytes]; // 开辟新空间
  // 每次分配返回的指针存到一个 vector 中,用一个数组记录了每次分配的情况
  blocks_.push_back(result);
  // memory_usage_ 记录了所有分配内存大小,包括刚分配的地址
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}
} // namespace leveldb
