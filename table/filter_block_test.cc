// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "gtest/gtest.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/testutil.h"

namespace leveldb {

// For testing: emit an array with one hash value per key
class TestHashFilter : public FilterPolicy {
 public:
  const char* Name() const override { return "TestHashFilter"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    for (int i = 0; i < n; i++) {
      uint32_t h = Hash(keys[i].data(), keys[i].size(), 1);
      PutFixed32(dst, h);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    uint32_t h = Hash(key.data(), key.size(), 1);
    for (size_t i = 0; i + 4 <= filter.size(); i += 4) {
      if (h == DecodeFixed32(filter.data() + i)) {
        return true;
      }
    }
    return false;
  }
};

class FilterBlockTest : public testing::Test {
 public:
  TestHashFilter policy_;
};

TEST_F(FilterBlockTest, EmptyBuilder) {
  FilterBlockBuilder builder(&policy_);
  Slice block = builder.Finish();
  ASSERT_EQ("\\x00\\x00\\x00\\x00\\x0b", EscapeString(block));
  FilterBlockReader reader(&policy_, block);
  ASSERT_TRUE(reader.KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(100000, "foo"));
}

TEST_F(FilterBlockTest, SingleChunk) {
  FilterBlockBuilder builder(&policy_);

  // 开始一个新的数据块（block_offset=100）
  // 计算出filter_index = 100/2048 = 0
  builder.StartBlock(100);

  // 添加键到当前过滤器
  builder.AddKey("foo");
  builder.AddKey("bar");
  builder.AddKey("box");
  // 由于block_offset=200和300都小于2048，所以都属于同一个过滤器索引0
  builder.StartBlock(200);
  builder.AddKey("box");
  builder.StartBlock(300);
  builder.AddKey("hello");
  // 完成过滤器块构建，生成最终的过滤器数据
  Slice block = builder.Finish();
  FilterBlockReader reader(&policy_, block);
  // block_offset=100 对应过滤器索引0，该过滤器包含所有添加的键
  ASSERT_TRUE(reader.KeyMayMatch(100, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "bar"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "box"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "hello"));
  ASSERT_TRUE(reader.KeyMayMatch(100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(100, "missing"));
  ASSERT_TRUE(!reader.KeyMayMatch(100, "other"));
}

TEST_F(FilterBlockTest, MultiChunk) {
  FilterBlockBuilder builder(&policy_);

  // First filter 创建第 1 个过滤器
  builder.StartBlock(0); //
  builder.AddKey("foo");
  // (2000/ 2048) = 0 , 还不需要创建过滤器
  builder.StartBlock(2000);
  builder.AddKey("bar");

  // Second filter
  /*
   * (3100/2048) = 1 > filter_offsets_.size() 创建一个过滤器
   * 现在 filter_offsets_.size() = 0
   * 1 >0 , 根据 [foo,bar] 创建一个过滤器
   */
  builder.StartBlock(3100); // 若条件符合，创建过滤器
  builder.AddKey("box");

  // Third filter is empty

  // Last filter
  // 9000/2048 = 4 ,前面会有4 个过滤器
  builder.StartBlock(9000); // 若条件符合，创建过滤器
  builder.AddKey("box");
  builder.AddKey("hello");

  Slice block = builder.Finish(); // 若条件符合，创建过滤器
  FilterBlockReader reader(&policy_, block);

  // Check first filter
  ASSERT_TRUE(reader.KeyMayMatch(0, "foo"));
  ASSERT_TRUE(reader.KeyMayMatch(2000, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(0, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(0, "hello"));

  ASSERT_TRUE(!reader.KeyMayMatch(2048, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(2048, "bar"));
  ASSERT_TRUE(reader.KeyMayMatch(2048, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(2048, "hello"));

  // Check second filter
  ASSERT_TRUE(reader.KeyMayMatch(3100, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(3100, "hello"));

  // Check third filter (empty)
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "bar"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "box"));
  ASSERT_TRUE(!reader.KeyMayMatch(4100, "hello"));

  // Check fourth filter (empty)
  ASSERT_TRUE(!reader.KeyMayMatch(8999, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(8999, "bar"));
  ASSERT_TRUE(reader.KeyMayMatch(8999, "box"));
  ASSERT_TRUE(reader.KeyMayMatch(8999, "hello"));

  // Check last filter
  ASSERT_TRUE(reader.KeyMayMatch(9000, "box"));
  ASSERT_TRUE(reader.KeyMayMatch(9000, "hello"));
  ASSERT_TRUE(!reader.KeyMayMatch(9000, "foo"));
  ASSERT_TRUE(!reader.KeyMayMatch(9000, "bar"));
}

}  // namespace leveldb
