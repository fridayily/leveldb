// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include "leveldb/slice.h"

#include "gtest/gtest.h"

namespace leveldb {

TEST(COMPARATOR_TEST,COM_EQ){
  const Comparator* bc = BytewiseComparator();
  Slice a("12345");
  Slice b("12345");
  std::cout << bc->Compare(a,b)  << std::endl;
  std::string s("12345");
  bc->FindShortestSeparator(&s,b);
  std::cout << "s " << s << std::endl;
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
  std::cout << "s " << s << std::endl;
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

  std::cout << "s " << s << std::endl;

}



}  // namespace leveldb
