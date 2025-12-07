// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include "leveldb/slice.h"

#include "gtest/gtest.h"

namespace leveldb {


class FindShortestSeparatorTest : public ::testing::Test {
public:
  const Comparator* comparator = BytewiseComparator();
};

TEST_F(FindShortestSeparatorTest,BasicFunctionality) {

  std::string start = "k01";
  Slice limit = "k03";
  comparator->FindShortestSeparator(&start,limit);
  EXPECT_EQ(start,"k03");
}

TEST_F(FindShortestSeparatorTest,AdjacentCharacters) {
  std::string start = "k03";
  Slice limit = "k04";
  comparator->FindShortestSeparator(&start,limit);
  EXPECT_EQ(start,"k03");
}

TEST_F(FindShortestSeparatorTest,PrefixRelationship) {

  std::string start = "hello";
  Slice limit = "hello world";
  comparator->FindShortestSeparator(&start,limit);
  EXPECT_EQ(start,"hello");
}

TEST_F(FindShortestSeparatorTest,ReversePrefixRelationship) {
  std::string start = "hello world";
  Slice limit = "hello";
  comparator->FindShortestSeparator(&start,limit);
  EXPECT_EQ(start,"hello world");
}

// 测试包含特殊字节的情况
TEST_F(FindShortestSeparatorTest, SpecialBytes) {
  std::string start = "key\xff\x01";
  Slice limit = "key\xff\x03";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, "key\xff\x02");
}

// 测试起始字符为0xff的情况
TEST_F(FindShortestSeparatorTest, StartWithFF) {
  std::string start = "key\xff";
  Slice limit = "key\xff\x01";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, "key\xff"); // 应该保持不变
}

// 测试相同字符串的情况
TEST_F(FindShortestSeparatorTest, IdenticalStrings) {
  std::string start = "hello";
  Slice limit = "hello";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, "hello"); // 应该保持不变
}

// 测试空字符串情况
TEST_F(FindShortestSeparatorTest, EmptyString) {
  std::string start;
  Slice limit = "a";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, ""); // 应该保持不变
}

// 测试普通差异情况
TEST_F(FindShortestSeparatorTest, NormalDifference) {
  std::string start = "abc";
  Slice limit = "azz";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, "ac"); // 应该变成"ab"
}

TEST_F(FindShortestSeparatorTest, FirstCharacterIncrement) {
  std::string start = "foo";
  Slice limit = "hello";
  comparator->FindShortestSeparator(&start, limit);
  EXPECT_EQ(start, "g");
}



TEST(COMPARATOR_TEST,COM_EQ){
  const Comparator* bc = BytewiseComparator();
  Slice a("12345");
  Slice b("12345");
  std::cout << bc->Compare(a,b)  << std::endl;
  std::string s("12345");
  bc->FindShortestSeparator(&s,b);
  ASSERT_EQ(s,"12345");
}


TEST(COMPARATOR_TEST,COM_B1){
  const Comparator* bc = BytewiseComparator();
  Slice a("12345");
  Slice b("12445");
  Slice c("12545");
  ASSERT_EQ(bc->Compare(a,b),-1);
  ASSERT_EQ(bc->Compare(a,c),-2);
}




TEST(COMPARATOR_TEST,COM_B4){
  const Comparator* bc = BytewiseComparator();
  Slice a("foo");
  Slice b("hello");
  std::cout << bc->Compare(a,b)  << std::endl;
  std::string s("foo");
  bc->FindShortestSeparator(&s,b);
  ASSERT_EQ(s,"g");
}

TEST(COMPARATOR_TEST,COM_B5){
  const Comparator* bc = BytewiseComparator();
  std::string a("K03");
  std::string b("K04");
  std::string c("K05");
  bc->FindShortestSeparator(&a,b);
  ASSERT_EQ(a,"K03");
  bc->FindShortestSeparator(&a,c);
  ASSERT_EQ(a,"K04");

}


TEST(COMPARATOR_TEST,FIND_SHORT){
  const Comparator* bc = BytewiseComparator();
  std::string s("1a2b3");
  bc->FindShortSuccessor(&s);
  ASSERT_EQ(s,"2");

}



}  // namespace leveldb
