// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "util/logging.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

class FindFileTest : public testing::Test {
 public:
  FindFileTest() : disjoint_sorted_files_(true) {}

  ~FindFileTest() {
    for (int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  // add 一次，添加一个文件
  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);  // 最小 key
    f->largest = InternalKey(largest, largest_seq, kTypeValue);     // 最大 key
    files_.push_back(f);  // 将 file 的元信息添加到files_中
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);         // 构造 key
    InternalKeyComparator cmp(BytewiseComparator());  // 构造比较器
    return FindFile(cmp, files_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, files_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }

  bool disjoint_sorted_files_;

 private:
  std::vector<FileMetaData*> files_;
};

TEST_F(FindFileTest, FileSet) {
  struct myCompare {
    bool operator()(const int& a, const int& b) const { return a > b; }
  };

  typedef std::set<int, myCompare> FileSet;
  constexpr myCompare comp;
  auto* f = new FileSet(comp);
  f->insert(1);
  f->insert(3);
  f->insert(5);
  f->insert(7);
  f->insert(9);
  f->insert(11);
  std::cout << f->size() << std::endl;
  std::cout << f->empty() << std::endl;
  // 查找第一个不小于给定值的元素
  const auto lower_it = f->lower_bound(4);
  std::cout << "*lower_it: " << *lower_it << std::endl;

  for (const int it : *f) {
    std::cout << it << " ";
  }
  std::cout << std::endl;

  delete f;
}

TEST_F(FindFileTest, Empty) {
  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(!Overlaps("a", "z"));
  ASSERT_TRUE(!Overlaps(nullptr, "z"));
  ASSERT_TRUE(!Overlaps("a", nullptr));
  ASSERT_TRUE(!Overlaps(nullptr, nullptr));
}

TEST_F(FindFileTest, Single) {
  // 添加一个文件
  Add("p", "q");
  ASSERT_EQ(0,
            Find("a"));  // 返回的是索引，在文件列表中查找第一个大于等于a的索引
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));  // q1 在 q 之外
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(!Overlaps("a", "b"));
  ASSERT_TRUE(!Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(!Overlaps(nullptr, "j"));
  ASSERT_TRUE(!Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindFileTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));  // 返回文件列表中第一个max_key大于 查找key 的索引
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("251", "299"));
  ASSERT_TRUE(!Overlaps("451", "500"));
  ASSERT_TRUE(!Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindFileTest, MultipleNullBoundaries) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(!Overlaps(nullptr, "149"));
  ASSERT_TRUE(!Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindFileTest, OverlapSequenceChecks) {
  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(!Overlaps("199", "199"));
  ASSERT_TRUE(!Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindFileTest, OverlappingFiles) {
  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;  // 文件之间有相交数据
  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files);

class AddBoundaryInputsTest : public testing::Test {
 public:
  std::vector<FileMetaData*> level_files_;
  std::vector<FileMetaData*> compaction_files_;
  std::vector<FileMetaData*> all_files_;
  InternalKeyComparator icmp_;

  AddBoundaryInputsTest() : icmp_(BytewiseComparator()) {}

  ~AddBoundaryInputsTest() {
    for (size_t i = 0; i < all_files_.size(); ++i) {
      delete all_files_[i];
    }
    all_files_.clear();
  }

  FileMetaData* CreateFileMetaData(uint64_t number, InternalKey smallest,
                                   InternalKey largest) {
    FileMetaData* f = new FileMetaData();
    f->number = number;
    f->smallest = smallest;
    f->largest = largest;
    all_files_.push_back(f);
    return f;
  }
};

TEST_F(AddBoundaryInputsTest, TestEmptyFileSets) {
  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_TRUE(compaction_files_.empty());
  ASSERT_TRUE(level_files_.empty());
}

TEST_F(AddBoundaryInputsTest, TestEmptyLevelFiles) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                         InternalKey(InternalKey("100", 1, kTypeValue)));
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(1, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_TRUE(level_files_.empty());
}

TEST_F(AddBoundaryInputsTest, TestEmptyCompactionFiles) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                         InternalKey(InternalKey("100", 1, kTypeValue)));
  level_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_TRUE(compaction_files_.empty());
  ASSERT_EQ(1, level_files_.size());
  ASSERT_EQ(f1, level_files_[0]);
}

TEST_F(AddBoundaryInputsTest, TestNoBoundaryFiles) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                         InternalKey(InternalKey("100", 1, kTypeValue)));
  FileMetaData* f2 =
      CreateFileMetaData(1, InternalKey("200", 2, kTypeValue),
                         InternalKey(InternalKey("200", 1, kTypeValue)));
  FileMetaData* f3 =
      CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
                         InternalKey(InternalKey("300", 1, kTypeValue)));

  level_files_.push_back(f3);
  level_files_.push_back(f2);
  level_files_.push_back(f1);
  compaction_files_.push_back(f2);
  compaction_files_.push_back(f3);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(2, compaction_files_.size());
}

TEST_F(AddBoundaryInputsTest, TestOneBoundaryFiles) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 3, kTypeValue),
                         InternalKey(InternalKey("100", 2, kTypeValue)));
  FileMetaData* f2 =
      CreateFileMetaData(1, InternalKey("100", 1, kTypeValue),
                         InternalKey(InternalKey("200", 3, kTypeValue)));
  FileMetaData* f3 =
      CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
                         InternalKey(InternalKey("300", 1, kTypeValue)));

  level_files_.push_back(f3);
  level_files_.push_back(f2);
  level_files_.push_back(f1);
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(2, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f2, compaction_files_[1]);
}

TEST_F(AddBoundaryInputsTest, TestTwoBoundaryFiles) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                         InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f2 =
      CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                         InternalKey(InternalKey("300", 1, kTypeValue)));
  FileMetaData* f3 =
      CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
                         InternalKey(InternalKey("100", 3, kTypeValue)));

  level_files_.push_back(f2);
  level_files_.push_back(f3);
  level_files_.push_back(f1);
  compaction_files_.push_back(f1);

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(3, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f3, compaction_files_[1]);
  ASSERT_EQ(f2, compaction_files_[2]);
}

TEST_F(AddBoundaryInputsTest, TestDisjoinFilePointers) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                         InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f2 =
      CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                         InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f3 =
      CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                         InternalKey(InternalKey("300", 1, kTypeValue)));
  FileMetaData* f4 =
      CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
                         InternalKey(InternalKey("100", 3, kTypeValue)));

  level_files_.push_back(f2);  // (100,6) (100,5)
  level_files_.push_back(f3);  // (100,2) (300,1)
  level_files_.push_back(f4);  // (100,4) (100,3)

  compaction_files_.push_back(f1);  // (100,6) (100,5)

  AddBoundaryInputs(icmp_, level_files_, &compaction_files_);
  ASSERT_EQ(3, compaction_files_.size());
  ASSERT_EQ(f1, compaction_files_[0]);
  ASSERT_EQ(f4, compaction_files_[1]);
  ASSERT_EQ(f3, compaction_files_[2]);
}

TEST_F(AddBoundaryInputsTest, TestGetOverlappingInputs) {
  FileMetaData* f1 =
      CreateFileMetaData(1, InternalKey("10", 1, kTypeValue),
                         InternalKey(InternalKey("20", 2, kTypeValue)));
  FileMetaData* f2 =
      CreateFileMetaData(2, InternalKey("15", 3, kTypeValue),
                         InternalKey(InternalKey("30", 4, kTypeValue)));

  std::vector<FileMetaData*> input;

  level_files_.push_back(f2);
  level_files_.push_back(f1);
  InternalKey begin = InternalKey("5", 5, kTypeValue);
  InternalKey end = InternalKey("15", 6, kTypeValue);

  std::string dbname_ = "/tmp/db_test";

  Options options_;
  InternalKeyComparator internal_comparator_(BytewiseComparator());
  //  table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
  //  // 这里也有个 LRU
  //
  VersionSet* vset_ =
      new VersionSet(dbname_, &options_, nullptr, &internal_comparator_);
  port::Mutex mutex_;
  mutex_.AssertHeld();

  VersionEdit edit;
  //  edit.SetNextFile(4);
  edit.SetNextFile(5);

  edit.SetLogNumber(1);
  edit.AddFile(0, 1, 20, InternalKey("10", 1, kTypeValue),
               InternalKey("20", 2, kTypeValue));
  edit.AddFile(0, 2, 20, InternalKey("15", 1, kTypeValue),
               InternalKey("30", 2, kTypeValue));
  vset_->SetLastSequence(3);
  vset_->LogAndApply(&edit, &mutex_);

  Version* v = vset_->current();
  v->GetOverlappingInputs(0, &begin, &end, &input);
  for (const auto& item : input) {
    std::cout << item->smallest.user_key().ToString() << std::endl;
  }
  //  GetOverlappingInputs();
}

}  // namespace leveldb
