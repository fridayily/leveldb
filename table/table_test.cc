// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include <map>
#include <string>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_builder.h"

#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/random.h"
#include "util/testutil.h"

#include "gtest/gtest.h"
#include "iterator_wrapper.h"

namespace leveldb {

// Return reverse of "key". 返回 key 的反转字符串
// Used to test non-lexicographic comparators. 用于测试非字典序比较器
static std::string Reverse(const Slice& key) {
  std::string str(key.ToString());
  std::string rev("");  // reverse_iterator 用于从后向前遍历
  for (std::string::reverse_iterator rit = str.rbegin(); rit != str.rend();
       ++rit) {
    rev.push_back(*rit);
  }
  return rev;
}

TEST(MyTest, Reverse) {
  std::string s = "abcde";
  std::cout << Reverse(s) << std::endl;
}

namespace {
class ReverseKeyComparator : public Comparator {
 public:
  const char* Name() const override {
    return "leveldb.ReverseBytewiseComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    BytewiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  void FindShortSuccessor(std::string* key) const override {
    std::string s = Reverse(*key);
    BytewiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};
}  // namespace
static ReverseKeyComparator reverse_key_comparator;

static void Increment(const Comparator* cmp, std::string* key) {
  if (cmp == BytewiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

// An STL comparator that uses a Comparator
namespace {
struct STLLessThan {
  const Comparator* cmp;
  // 默认构造函数，比较器用 BytewiseComparator
  // 有参构造函数，传入比较器函数
  STLLessThan() : cmp(BytewiseComparator()) {}
  STLLessThan(const Comparator* c) : cmp(c) {}
  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
}  // namespace

class StringSink : public WritableFile {
 public:
  ~StringSink() override = default;

  const std::string& contents() const { return contents_; }

  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }

  Status Append(const Slice& data) override {
    contents_.append(data.data(), data.size());
    return Status::OK();
  }

 private:
  std::string contents_;
};

class StringSource : public RandomAccessFile {
 public:
  StringSource(const Slice& contents)
      : contents_(contents.data(), contents.size()) {}

  ~StringSource() override = default;

  uint64_t Size() const { return contents_.size(); }

  // 将数据读取到 scratch 指向的内存中， result 指向 scratch
  // 的地址，只是进行类型转换
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset >= contents_.size()) {
      return Status::InvalidArgument("invalid Read offset");
    }
    if (offset + n >
        contents_.size()) {  // 如果读取的长度超过文件长度，则只读取到文件结尾
      n = contents_.size() - offset;
    }
    std::memcpy(scratch, &contents_[offset], n);
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  std::string contents_;
};

typedef std::map<std::string, std::string, STLLessThan> KVMap;

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp) : data_(STLLessThan(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, std::vector<std::string>* keys,
              KVMap* kvmap) {
    *kvmap = data_;
    keys->clear();
    for (const auto& kvp : data_) {  // 将key 添加到keys
      keys->push_back(kvp.first);
    }
    data_.clear();  // data 清空后 kvmap 还有数据,因为 *kvmap = data_ 是值拷贝
    Status s = FinishImpl(options, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options, const KVMap& data) = 0;

  virtual Iterator* NewIterator() const = 0;

  const KVMap& data() const { return data_; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

 private:
  KVMap data_;
};

class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() override { delete block_; }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(&options);

    for (const auto& kvp : data) {
      builder.Add(kvp.first, kvp.second);
    }
    // Open the block
    // data 指 encode 后的 k、v 和重启点数组信息
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    contents.cachable = false;
    contents.heap_allocated = false;
    block_ = new Block(contents);
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return block_->NewIterator(comparator_);
  }

 private:
  const Comparator* const comparator_;
  std::string data_;
  Block* block_;

  BlockConstructor();
};

class TableConstructor : public Constructor {
 public:
  TableConstructor(const Comparator* cmp)
      : Constructor(cmp), source_(nullptr), table_(nullptr) {}
  ~TableConstructor() override { Reset(); }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    Reset();
    StringSink sink;
    TableBuilder builder(options, &sink);

    for (const auto& kvp : data) {
      builder.Add(kvp.first, kvp.second);
      EXPECT_LEVELDB_OK(builder.status());
    }
    Status s = builder.Finish();
    EXPECT_LEVELDB_OK(s);

    // 比较内容大小和索引偏移
    EXPECT_EQ(sink.contents().size(), builder.FileSize());

    // Open the table
    source_ = new StringSource(sink.contents());
    Options table_options;
    table_options.comparator = options.comparator;
    // 检查刚写入的table 数据是否合法
    return Table::Open(table_options, source_, sink.contents().size(), &table_);
  }

  Iterator* NewIterator() const override {
    return table_->NewIterator(ReadOptions());
  }

  uint64_t ApproximateOffsetOf(const Slice& key) const {
    return table_->ApproximateOffsetOf(key);
  }

 private:
  void Reset() {
    delete table_;
    delete source_;
    table_ = nullptr;
    source_ = nullptr;
  }

  StringSource* source_;
  Table* table_;

  TableConstructor();
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public Iterator {
 public:
  explicit KeyConvertingIterator(Iterator* iter) : iter_(iter) {}

  KeyConvertingIterator(const KeyConvertingIterator&) = delete;
  KeyConvertingIterator& operator=(const KeyConvertingIterator&) = delete;

  ~KeyConvertingIterator() override { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey key;
    if (!ParseInternalKey(iter_->key(), &key)) {
      status_ = Status::Corruption("malformed internal key");
      return Slice("corrupted key");
    }
    return key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  Iterator* iter_;
};

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp)
      : Constructor(cmp), internal_comparator_(cmp) {
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
  }
  ~MemTableConstructor() override { memtable_->Unref(); }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    memtable_->Unref();
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
    int seq = 1;
    for (const auto& kvp : data) {
      memtable_->Add(seq, kTypeValue, kvp.first, kvp.second);
      seq++;
    }
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return new KeyConvertingIterator(memtable_->NewIterator());
  }

 private:
  const InternalKeyComparator internal_comparator_;
  MemTable* memtable_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp) {
    db_ = nullptr;
    NewDB();
  }
  ~DBConstructor() override { delete db_; }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    delete db_;
    db_ = nullptr;
    NewDB();
    for (const auto& kvp : data) {
      WriteBatch batch;
      batch.Put(kvp.first, kvp.second);
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return db_->NewIterator(ReadOptions());
  }

  DB* db() const override { return db_; }

 private:
  void NewDB() {
    std::string name = testing::TempDir() + "leveldb/table_testdb";

    Options options;
    options.comparator = comparator_;
    Status status = DestroyDB(name, options);
    ASSERT_TRUE(status.ok()) << status.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;  // Something small to force merging
    status = DB::Open(options, name, &db_);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  const Comparator* const comparator_;
  DB* db_;
};

enum TestType { TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST };

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_interval;
};

static const TestArgs kTestArgList[] = {
    {TABLE_TEST, false, 16},
    {TABLE_TEST, false, 1},
    {TABLE_TEST, false, 1024},
    {TABLE_TEST, true, 16},
    {TABLE_TEST, true, 1},
    {TABLE_TEST, true, 1024},

    {BLOCK_TEST, false, 16},
    {BLOCK_TEST, false, 1},
    {BLOCK_TEST, false, 1024},
    {BLOCK_TEST, true, 16},
    {BLOCK_TEST, true, 1},
    {BLOCK_TEST, true, 1024},

    // Restart interval does not matter for memtables
    {MEMTABLE_TEST, false, 16},
    {MEMTABLE_TEST, true, 16},

    // Do not bother with restart interval variations for DB
    {DB_TEST, false, 16},
    {DB_TEST, true, 16},
};
static const int kNumTestArgs = sizeof(kTestArgList) / sizeof(kTestArgList[0]);

class Harness : public testing::Test {
 public:
  Harness() : constructor_(nullptr) {}

  void Init(const TestArgs& args) {
    delete constructor_;
    constructor_ = nullptr;
    options_ = Options();

    // 自己添加，不使用压缩测试
    options_.compression = kNoCompression;

    options_.block_restart_interval = args.restart_interval;  // 设置重启间隔
    // Use shorter block size for tests to exercise block boundary
    // conditions more.
    options_.block_size = 256;
    if (args.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }
    switch (args.type) {
      case TABLE_TEST:
        constructor_ = new TableConstructor(options_.comparator);
        break;
      case BLOCK_TEST:
        constructor_ = new BlockConstructor(options_.comparator);
        break;
      case MEMTABLE_TEST:
        constructor_ = new MemTableConstructor(options_.comparator);
        break;
      case DB_TEST:
        constructor_ = new DBConstructor(options_.comparator);
        break;
    }
  }

  ~Harness() { delete constructor_; }

  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(Random* rnd) {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "constructor finished");
    TestForwardScan(keys, data);
    TestBackwardScan(keys, data);
    TestRandomAccess(rnd, keys, data);
  }

  void TestForward() {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "constructor finished");
    TestForwardScan(keys, data);
  }

  void TestBackward() {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "constructor finished");
    TestBackwardScan(keys, data);
  }

  void TestRandom(Random* rnd) {
    std::vector<std::string> keys;
    KVMap data;
    constructor_->Finish(options_, &keys, &data);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "constructor finished");
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string>& keys,
                       const KVMap& data) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "start, create iter");
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());  // 迭代器无效才继续执行
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToFirst begin");

    iter->SeekToFirst();
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToFirst end. iter begin");
    for (KVMap::const_iterator model_iter = data.begin();
         model_iter != data.end(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "iter->next");
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete iter begin");
    delete iter;
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete iter end");
  }

  void TestBackwardScan(const std::vector<std::string>& keys,
                        const KVMap& data) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "start, create iter");
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToLast begin");

    iter->SeekToLast();
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "SeekToLast end. iter begin");
    for (KVMap::const_reverse_iterator model_iter = data.rbegin();
         model_iter != data.rend(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "iter->prev");
      iter->Prev();
    }
    ASSERT_TRUE(!iter->Valid());
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete iter begin");
    delete iter;
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "delete iter end");
  }

  void TestRandomAccess(Random* rnd, const std::vector<std::string>& keys,
                        const KVMap& data) {
    static const bool kVerbose = false;
    Iterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    KVMap::const_iterator model_iter = data.begin();
    if (kVerbose) std::fprintf(stderr, "---\n");
    for (int i = 0; i < 200; i++) {
      const int toss = rnd->Uniform(5);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Next\n");
            iter->Next();
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 1: {
          if (kVerbose) std::fprintf(stderr, "SeekToFirst\n");
          iter->SeekToFirst();
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose)
            std::fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          iter->Seek(Slice(key));
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 3: {
          if (iter->Valid()) {
            if (kVerbose) std::fprintf(stderr, "Prev\n");
            iter->Prev();
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 4: {
          if (kVerbose) std::fprintf(stderr, "SeekToLast\n");
          iter->SeekToLast();
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    delete iter;
  }

  std::string ToString(const KVMap& data, const KVMap::const_iterator& it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const KVMap& data,
                       const KVMap::const_reverse_iterator& it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const Iterator* it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random* rnd, const std::vector<std::string>& keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(keys.size());
      std::string result = keys[index];
      switch (rnd->Uniform(3)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (!result.empty() && result[result.size() - 1] > '\0') {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Returns nullptr if not running against a DB
  DB* db() const { return constructor_->db(); }

 private:
  Options options_;
  Constructor* constructor_;
};

class ModelTestIter : public Iterator {
 public:
  ModelTestIter(const KVMap* map, bool owned)
      : map_(map), owned_(owned), iter_(map_->end()) {}
  ~ModelTestIter() override {
    if (owned_) delete map_;
  }
  bool Valid() const override { return iter_ != map_->end(); }
  void SeekToFirst() override { iter_ = map_->begin(); }
  void SeekToLast() override {
    if (map_->empty()) {
      iter_ = map_->end();
    } else {
      // rbegin() 返回一个逆向迭代器
      iter_ = map_->find(map_->rbegin()->first);
    }
  }
  void Seek(const Slice& k) override {
    // 用于查找第一个键值不小于给定值的元素。这个方法返回一个迭代器，指向该元素。
    // 如果所有键都小于给定值，则返回 map 的 end 迭代器。
    iter_ = map_->lower_bound(k.ToString());
  }
  void Next() override { ++iter_; }
  void Prev() override { --iter_; }
  Slice key() const override { return iter_->first; }
  Slice value() const override { return iter_->second; }
  Status status() const override { return Status::OK(); }

 private:
  const KVMap* const map_;
  const bool owned_;            // Do we own map_
  KVMap::const_iterator iter_;  // 只允许读取容器中的元素，不能修改
};

TEST(TestIter, ModelTestIter) {
  KVMap data;
  data.insert({"a", "1"});
  data.insert({"b", "2"});
  data.insert({"c", "3"});
  data.insert({"d", "4"});
  data.insert({"e", "5"});
  ModelTestIter testIter(&data, false);

  testIter.Seek("b");
  // 获取key 要虚函数调用
  std::cout << testIter.key().data() << ": " << testIter.value().ToString()
            << std::endl;
  testIter.Next();
  std::cout << testIter.key().data() << ": " << testIter.value().ToString()
            << std::endl;

  IteratorWrapper iteratorWrapper(&testIter);
  // key 已经缓存，每次取key 不需要经历虚函数调用，但取 value
  // 每次还是需要虚函数调用
  std::cout << iteratorWrapper.key().ToString() << ": "
            << iteratorWrapper.value().ToString() << std::endl;
  iteratorWrapper.Next();
  std::cout << iteratorWrapper.key().ToString() << ": "
            << iteratorWrapper.value().ToString() << std::endl;
  std::cout << "valid: " << iteratorWrapper.Valid() << std::endl;
}

// Test empty table/block.
TEST_F(Harness, Empty) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Test(&rnd);
  }
}

// Special test for a block with no restart entries.  The C++ leveldb
// code never generates such blocks, but the Java version of leveldb
// seems to.
TEST_F(Harness, ZeroRestartPointsInBlock) {
  char data[sizeof(uint32_t)];
  memset(data, 0, sizeof(data));
  BlockContents contents;
  contents.data = Slice(data, sizeof(data));
  contents.cachable = false;
  contents.heap_allocated = false;
  Block block(contents);
  Iterator* iter = block.NewIterator(BytewiseComparator());
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(!iter->Valid());
  iter->Seek("foo");
  ASSERT_TRUE(!iter->Valid());
  delete iter;
}

// Test the empty key
TEST_F(Harness, SimpleEmptyKey) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 1);
    Add("", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSingle) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 2);
    Add("abc", "v");
    Test(&rnd);
  }
}

TEST_F(Harness, RestartTableTest) {
  TestArgs args = {TABLE_TEST, false, 2};
  Init(args);
  Random rnd(test::RandomSeed() + 3);
  Add("abc", "v1");
  Add("abcd", "v2");
  Add("abcde", "v3");
  Add("abcdef", "v4");
  Add("abcdefg", "v5");
  Test(&rnd);
}

TEST_F(Harness, DBTest) {
  TestArgs args = {DB_TEST, false, 2};
  Init(args);
  Random rnd(test::RandomSeed() + 3);
  Add("abc", "v1");
  Add("abcd", "v2");
  Add("abcde", "v3");
  Add("abcdef", "v4");
  Add("abcdefg", "v5");
  Test(&rnd);
}

TEST_F(Harness, SelfArgsTest) {
  for (int i = 0; i < kNumTestArgs; i++) {
    std::cout << "i: " << i << std::endl;
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v1");
    Add("abcd", "v2");
    Add("abcde", "v3");
    Add("abcdef", "v4");
    Add("abcdefg", "v5");
    Test(&rnd);
  }
}

TEST_F(Harness, ForwardScan) {
  Init(kTestArgList[6]);
  Add("abc", "v");
  Add("abcd", "v");
  Add("ac", "v2");
  TestForward();
}

TEST_F(Harness, BackwardScan) {
  Init(kTestArgList[0]);
  Add("abc", "v");
  Add("abcd", "v");
  Add("ac", "v2");
  TestBackward();
}

TEST_F(Harness, RandomScan) {
  Init(kTestArgList[0]);
  Random rnd(test::RandomSeed() + 3);
  Add("abc", "v");
  Add("abcd", "v");
  Add("ac", "v2");
  TestRandom(&rnd);
}

TEST_F(Harness, SimpleMulti) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v");
    Add("abcd", "v");
    Add("ac", "v2");
    Test(&rnd);
  }
}

TEST_F(Harness, SimpleSpecialKey) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 4);
    Add("\xff\xff", "v3");
    Test(&rnd);
  }
}

TEST_F(Harness, SelfRandomized) {
  int i = 0;
  Init(kTestArgList[i]);
  Random rnd(test::RandomSeed() + 5);
  for (int num_entries = 0; num_entries < 2000;
       num_entries += (num_entries < 50 ? 1 : 200)) {
    if ((num_entries % 10) == 0) {
      std::fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                   int(kNumTestArgs), num_entries);
    }
    for (int e = 0; e < num_entries; e++) {
      std::string v;
      Add(test::RandomKey(&rnd, rnd.Skewed(4)),
          test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
    }
    Test(&rnd);
  }
}

TEST_F(Harness, Randomized) {
  for (int i = 0; i < kNumTestArgs; i++) {
    Init(kTestArgList[i]);
    Random rnd(test::RandomSeed() + 5);
    for (int num_entries = 0; num_entries < 2000;
         num_entries += (num_entries < 50 ? 1 : 200)) {
      if ((num_entries % 10) == 0) {
        std::fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                     int(kNumTestArgs), num_entries);
      }
      for (int e = 0; e < num_entries; e++) {
        std::string v;
        Add(test::RandomKey(&rnd, rnd.Skewed(4)),
            test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
      }
      Test(&rnd);
    }
  }
}

TEST_F(Harness, RandomizedLongDB) {
  Random rnd(test::RandomSeed());
  TestArgs args = {DB_TEST, false, 16};
  Init(args);
  int num_entries = 100000;
  for (int e = 0; e < num_entries; e++) {
    std::string v;
    Add(test::RandomKey(&rnd, rnd.Skewed(4)),
        test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
  }
  Test(&rnd);

  // We must have created enough data to force merging
  int files = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    std::string value;
    char name[100];
    std::snprintf(name, sizeof(name), "leveldb.num-files-at-level%d", level);
    ASSERT_TRUE(db()->GetProperty(name, &value));
    std::cout << "name: " << name << std::endl;
    std::cout << "value: " << value << std::endl;
    files += atoi(value.c_str());
  }
  ASSERT_GT(files, 0);
}

TEST(MemTableTest, Simple) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* memtable = new MemTable(cmp);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("largekey"), std::string("vlarge"));
  ASSERT_TRUE(WriteBatchInternal::InsertInto(&batch, memtable).ok());

  Iterator* iter = memtable->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    std::fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
                 iter->value().ToString().c_str());
    iter->Next();
  }

  delete iter;
  memtable->Unref();
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    std::fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
                 (unsigned long long)(val), (unsigned long long)(low),
                 (unsigned long long)(high));
  }
  return result;
}

TEST(TableTest, ApproximateOffsetOfPlain) {
  TableConstructor c(BytewiseComparator());
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = kNoCompression;
  c.Finish(options, &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 612000));
}

static bool SnappyCompressionSupported() {
  std::string out;
  Slice in = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  return port::Snappy_Compress(in.data(), in.size(), &out);
}

TEST(TableTest, ApproximateOffsetOfCompressed) {
  if (!SnappyCompressionSupported())
    GTEST_SKIP() << "skipping compression tests";

  Random rnd(301);
  TableConstructor c(BytewiseComparator());
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  KVMap kvmap;
  Options options;
  options.block_size = 1024;
  options.compression = kSnappyCompression;
  c.Finish(options, &keys, &kvmap);

  // Expected upper and lower bounds of space used by compressible strings.
  static const int kSlop = 1000;  // Compressor effectiveness varies.
  const int expected = 2500;      // 10000 * compression ratio (0.25)
  const int min_z = expected - kSlop;
  const int max_z = expected + kSlop;

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, kSlop));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, kSlop));
  // Have now emitted a large compressible string, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), min_z, max_z));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), min_z, max_z));
  // Have now emitted two large compressible strings, so adjust expected offset.
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 2 * min_z, 2 * max_z));
}

}  // namespace leveldb
