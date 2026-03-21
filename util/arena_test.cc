// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

#include "util/random.h"

#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"
#include "spdlog/common.h"

namespace leveldb {

TEST(ArenaTest, Empty) { Arena arena; }

bool IsInRange(char* ptr, char* lower, char* upper) {
  return ptr > lower && ptr < upper;
}

TEST(ArenaTest, BasicAllocation) {
  // 创建Arena实例
  Arena arena;

  // 1. 测试首次分配（小内存）
  // 预期：分配一个kBlockSize大小的块（默认4096字节）
  char* ptr1 = arena.Allocate(100);
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "alloc ptr1 {:p}", ptr1);

  // 在堆中分配内存，用来测试下面的 Allocate(3000) 分配的内存和 ptr1 是不连续的
  std::vector<int> temp(2048);

  // 验证分配成功
  ASSERT_NE(ptr1, nullptr);

  // 向分配的内存写入数据
  for (size_t i = 0; i < 100; ++i) {
    ptr1[i] = static_cast<char>(i);
  }

  // 验证数据正确性
  for (size_t i = 0; i < 100; ++i) {
    ASSERT_EQ(ptr1[i], static_cast<char>(i));
  }

  // 2. 测试消耗大部分内存
  // 分配足够的内存，使当前块剩余空间不足2000字节
  char* ptr2 = arena.Allocate(
      3000);  // 3000 + 100 = 3100 < 4096，当前块还有4096-3100=996字节剩余
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "alloc ptr2 {:p}", ptr2);

  // 向分配的内存写入数据
  for (size_t i = 0; i < 3000; ++i) {
    ptr2[i] = static_cast<char>(i);
  }

  for (size_t i = 0; i < 3000; ++i) {
    ASSERT_EQ(ptr2[i], static_cast<char>(i));
  }

  // 验证分配成功
  ASSERT_NE(ptr2, nullptr);

  // 3. 测试大内存分配（大于kBlockSize/4且当前块空间不足）
  // 预期：单独分配一个刚好满足大小的块
  char* ptr3 = arena.Allocate(2000);  // 2000 > 1024，且当前块剩余996 < 2000
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "alloc ptr3 {:p}", ptr3);

  // 向分配的内存写入数据
  for (size_t i = 0; i < 2000; ++i) {
    ptr3[i] = static_cast<char>(i);
  }

  for (size_t i = 0; i < 2000; ++i) {
    ASSERT_EQ(ptr3[i], static_cast<char>(i));
  }

  // 验证分配成功且不是从当前块分配（ptr3不应该在ptr2之后）
  ASSERT_NE(ptr3, nullptr);
  ASSERT_NE(ptr3, ptr2 + 3000);  // 验证不是从当前块分配

  // 4. 测试内存使用统计
  // 预期：至少分配了100 + 3000 + 2000 = 5100字节
  ASSERT_GE(arena.MemoryUsage(), 5100);

  // 5. 测试内存对齐分配
  char* aligned_ptr = arena.AllocateAligned(100);
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "alloc aligned_ptr {:p}", aligned_ptr);

  // 验证分配成功且地址对齐
  ASSERT_NE(aligned_ptr, nullptr);
  ASSERT_EQ(
      reinterpret_cast<uintptr_t>(aligned_ptr) % alignof(std::max_align_t), 0);
  ASSERT_PRED3(IsInRange, aligned_ptr, ptr1 + 3100, ptr1 + 4096);
}



TEST(Random, OneIn) {
  Random rnd(0xdeadbeef);
  for (int i = 0; i < 20; i++) {
    std::cout << rnd.OneIn(4) << " ";
  }
  std::cout << std::endl;
}

TEST(ArenaTest, Simple) {
  std::vector<std::pair<size_t, char*>> allocated;
  Arena arena;
  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000)
              ? rnd.Uniform(6000)
              : (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    if (rnd.OneIn(10)) {  // 10分之1的概率返回 true
      r = arena.AllocateAligned(s);
    } else {
      r = arena.Allocate(s);
    }

    for (size_t b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      // 向刚分配的内存中写数据
      r[b] = i % 256;
    }
    bytes += s;
    // bytes 总的分配字节数
    // s 当次分配的字节数
    // r 当次分配的地址
    allocated.push_back(std::make_pair(s, r));
    ASSERT_GE(arena.MemoryUsage(), bytes);
    if (i > N / 10) {
      ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
    }
  }
  for (size_t i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;
    for (size_t b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }

}  // namespace leveldb

}  // namespace leveldb