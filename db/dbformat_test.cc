// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include "gtest/gtest.h"
#include "util/logging.h"

namespace leveldb {

static std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded; // 长度为 user_key + seq + vt
}

static std::string Shorten(const std::string& s, const std::string& l) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortestSeparator(&result, l);
  return result;
}

static std::string ShortSuccessor(const std::string& s) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortSuccessor(&result);
  return result;
}

static void TestKey(const std::string& key, uint64_t seq, ValueType vt) {
  std::string encoded = IKey(key, seq, vt);

  Slice in(encoded); // key + seq + valueType
  ParsedInternalKey decoded("", 0, kTypeValue); // 初始化解码器

  ASSERT_TRUE(ParseInternalKey(in, &decoded)); // 将encoded 解码存到decoded 中
  ASSERT_EQ(key, decoded.user_key.ToString());
  ASSERT_EQ(seq, decoded.sequence);
  ASSERT_EQ(vt, decoded.type);

  ASSERT_TRUE(!ParseInternalKey(Slice("bar"), &decoded));
}

TEST(FormatTest, InternalKey_EncodeDecode) {
  const char* keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
  const uint64_t seq[] = {1,
                          2,
                          3,
                          (1ull << 8) - 1,
                          1ull << 8,
                          (1ull << 8) + 1,
                          (1ull << 16) - 1,
                          1ull << 16,
                          (1ull << 16) + 1,
                          (1ull << 32) - 1,
                          1ull << 32,
                          (1ull << 32) + 1};
  for (int k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
    for (int s = 0; s < sizeof(seq) / sizeof(seq[0]); s++) {
      TestKey(keys[k], seq[s], kTypeValue); // kTypeValue 标识插入key,
      TestKey("hello", 1, kTypeDeletion); // kTypeDeletion 标识删除key
    }
  }
}

TEST(FormatTest,MyTest){
    InternalKey key = InternalKey(InternalKey("100", 1, kTypeValue));
    Slice s = key.Encode();



    std::cout << "s " << s.data() << std::endl;


    std::string s1 = std::string(std::string ("hello"));

}

TEST(FormatTest, InternalKey_DecodeFromEmpty) {
  InternalKey internal_key;

  ASSERT_TRUE(!internal_key.DecodeFrom(""));
}

TEST(FormatTest, InternalKeyShortSeparator) {
  // When user keys are same
  ASSERT_EQ(IKey("foo", 100, kTypeValue),  // Ikey 返回的是字符串，所以可以比较，这里比较 user_key + seq + value_type
            Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 99, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 101, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeDeletion)));

  // When user keys are misordered
  ASSERT_EQ(IKey("foo", 100, kTypeValue),
            Shorten(IKey("foo", 100, kTypeValue), IKey("bar", 99, kTypeValue)));

  // When user keys are different, but correctly ordered
  // FindShortestSeparator(&result, l)-> foo 与 hello 比较, f 与 h 差值大于1，所以 result 变成 g
  ASSERT_EQ(
      IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("foo", 100, kTypeValue), IKey("hello", 200, kTypeValue)));

  // When start user key is prefix of limit user key
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foobar", 200, kTypeValue)));

  // When limit user key is prefix of start user key
  ASSERT_EQ(
      IKey("foobar", 100, kTypeValue),
      Shorten(IKey("foobar", 100, kTypeValue), IKey("foo", 200, kTypeValue)));
}

TEST(FormatTest, InternalKeyShortestSuccessor) {
  ASSERT_EQ(IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
            ShortSuccessor(IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(IKey("\xff\xff", 100, kTypeValue),
            ShortSuccessor(IKey("\xff\xff", 100, kTypeValue)));
}

TEST(FormatTest, ParsedInternalKeyDebugString) {
  ParsedInternalKey key("The \"key\" in 'single quotes'", 42, kTypeValue);

  ASSERT_EQ("'The \"key\" in 'single quotes'' @ 42 : 1", key.DebugString());
}

TEST(FormatTest, InternalKeyDebugString) {
  InternalKey key("The \"key\" in 'single quotes'", 42, kTypeValue);
  ASSERT_EQ("'The \"key\" in 'single quotes'' @ 42 : 1", key.DebugString());

  InternalKey invalid_key;
  ASSERT_EQ("(bad)", invalid_key.DebugString());
}

}  // namespace leveldb
