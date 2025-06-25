// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/skiplist.h"

#include <atomic>
#include <set>

#include "leveldb/env.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

typedef uint64_t Key;

struct Comparator {
  int operator()(const Key& a, const Key& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

TEST(MyTest, Rand) {
  Random r(10);
  std::cout << r.Next() << std::endl;
  std::cout << r.Next() << std::endl;
  std::cout << r.Next() << std::endl;
  std::cout << r.Uniform(1) << std::endl;
  std::cout << r.Uniform(10) << std::endl;
  std::cout << r.Uniform(100) << std::endl;

  std::cout << r.Skewed(10) << std::endl;
}

TEST(SkipTest, Empty) {
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
  ASSERT_TRUE(!list.Contains(10));

  SkipList<Key, Comparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek(100);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST(SkipTest, My) {
  std::set<Key> keys;
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);

  list.Insert(3);
  list.Insert(6);
  list.Insert(7);
  list.Insert(9);
  list.Insert(12);
  list.Insert(19);
  list.Insert(21);
  list.Insert(25);
  list.Insert(26);
  list.Insert(17);

  SkipList<Key, Comparator>::Iterator iter(&list);

  iter.SeekToFirst();
  while (iter.Valid()) {
    printf("-> %llu ", iter.key());
    iter.Next();
  }
  printf("\n");
  iter.SeekToLast();
  std::cout << "key: " << iter.key() << std::endl;
  iter.Prev();
  std::cout << "key: " << iter.key() << std::endl;

  iter.Next();
  std::cout << "key: " << iter.key() << std::endl;
}

TEST(SkipTest, InsertAndLookup) {
  // 判断为 false 时输出 100
  ASSERT_TRUE(1 == 1) << 100;

  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;  // set 中元素已排序
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    // keys.insert 返回 std::pair<iterator, bool>
    if (keys.insert(key).second) {  // 插入 keys,便于比较
      list.Insert(key);             // 插入 skip_list
    }
  }

  for (int i = 0; i < R; i++) {
    if (list.Contains(i)) {
      ASSERT_EQ(keys.count(i), 1);
    } else {
      ASSERT_EQ(keys.count(i), 0);
    }
  }

  // Simple iterator tests
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    iter.Seek(0);  // 查找大于等于0的第一个结点
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToFirst();  // 定位到第一个结点
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }

  // Forward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    // lower_bound 在有序序列中查找第一个大于等于 i 的地址
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToLast();
    // 逆序查找,只有一条线路
    // Compare against model iterator
    for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
         model_iter != keys.rend(); ++model_iter) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, iter.key());
      iter.Prev();
    }
    ASSERT_TRUE(!iter.Valid());
  }
}

// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructed.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
//  当迭代器被构造时, reader 总是能观察到跳跃表中的所有数据
//  因为插入时并发的,
//  当迭代器创建后,我们或许可以观察到当迭代器创建之后插入当新值
//  但我们永远不会错过迭代器创建期间出现的值
//
//  创建一个由多个部分组成的 key
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
//  插入代码随机抽取一个随机的key, 根据key生成一个数并加上1 作为 gen 值
//  最后 hash 值为 Hash(key,gen)
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
class ConcurrentTest {
 private:
  static constexpr uint32_t K = 4;

  static uint64_t key(Key key) { return (key >> 40); }
  static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
  static uint64_t hash(Key key) { return key & 0xff; }

  /* 将 k 和 g 组合成一个key进行hash */
  static uint64_t HashNumbers(uint64_t k, uint64_t g) {
    uint64_t data[2] = {k, g};
    return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
  }

  /* 将 k,g,hash 都保存在一个Key 里*/
  static Key MakeKey(uint64_t k, uint64_t g) {
    static_assert(sizeof(Key) == sizeof(uint64_t), "");
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);  // g为小于32位无符号整数,
                               // 后面左移8位,所以第8位到第39位存的下g
    // 返回值由3部分组成
    // 40-63 位是k , 8-39 为 g , 0-7 为 HashNumbers
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }

  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
  }

  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }

  // Per-key generation
  struct State {
    /* 一个有原子变量组成的数组 */
    std::atomic<int> generation[K];
    void Set(int k, int v) {
      generation[k].store(v, std::memory_order_release);
    }
    int Get(int k) { return generation[k].load(std::memory_order_acquire); }

    State() {
      for (int k = 0; k < K; k++) {
        Set(k, 0);
      }
    }
  };

  // Current state of the test
  State current_;

  Arena arena_;

  // SkipList is not protected by mu_.  We just use a single writer
  // thread to modify it.
  SkipList<Key, Comparator> list_;

 public:
  Logger* info_log;
  ConcurrentTest() : list_(Comparator(), &arena_) {}

  // REQUIRES: External synchronization
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const intptr_t g = current_.Get(k) + 1;  // current_ 是 ReadStep 中写的
    // 创建一个 key
    // printf("WriteStep: 随机生成 k=%x gh=%lx,下一步 MakeKey\n",k,g);

    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "k {} g {}", k, g);

    const Key key = MakeKey(k, g);

    // printf("WriteStep: 将 Key=%llx 写入跳跃表 list_.Insert\n",key);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "insert key {} to SkipList ", key);

    list_.Insert(key);  // 写跳跃表

    SkipList<Key, Comparator>::Iterator iter(&list_);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "begin iter SkipList ");

    iter.SeekToFirst();
    while (iter.Valid()) {
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "iter key {}", key);
      iter.Next();
    }

    // printf("WriteStep: k=%d, g=%ld 写入 current_.Set(k,g)\n",k,g);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "current.Set k {} g {}", k, g);
    current_.Set(k, g);  // 更新 current_中对应的值
  }

  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "ReadStep start");

    State initial_state;

    for (int k = 0; k < K; k++) {
      // 将存在 current_ 数组中的值复制到 initial_state 中
      initial_state.Set(k, current_.Get(k));
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "initial_state end");

    Key pos = RandomTarget(rnd);
    // printf(
    //     "ReadStep: 生成随机值 pos=%llx,在表中查找第一个大于该值的
    //     entry,下一步" "初始化迭代器,并将迭代器定位到 pos 后一个位置 \n",
    //     pos);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "RandomTarget pos {}", pos);

    SkipList<Key, Comparator>::Iterator iter(&list_);

    iter.Seek(pos);  // 前进到第一个大于 pos 的 key
    while (true) {
      // printf("新建Key对象 current\n");
      Key current;
      if (!iter.Valid()) {
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "invalid iter: MakeKey({},0)", K);

        current = MakeKey(K, 0);
        // printf("ReadStep: 迭代器无效 current=%llx current 设为最大值\n",
        //        current);

      } else {
        // 前进到第一个大于 pos 的 key 成功,获取对应的 entry
        current = iter.key();
        // printf("ReadStep: 迭代器有效 获取值成功 pos=%llx, current=%llx\n",
        // pos,
        //        current);
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "valid iter: pos={} current={}",
                           pos, current);

        ASSERT_TRUE(IsValidKey(current)) << current;
      }
      ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state. 验证在[pos,current_) 区间的值没出现在 initial_state 中
      while (pos < current) {
        // printf("ReadStep: pos < current ,pos = %llx,current=%llx\n", pos,
        //        current);
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "pos < current: pos={} current={}",
                           pos, current);

        ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        ASSERT_TRUE((gen(pos) == 0) ||
                    (gen(pos) > static_cast<Key>(initial_state.Get(key(pos)))))
            << "key: " << key(pos) << "; gen: " << gen(pos)
            << "; initgen: " << initial_state.Get(key(pos));

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          // printf(
          //     "ReadStep: key(pos) < key(current) ,pos = %llx,current=%llx "
          //     "需要创建新key\n",
          //     pos, current);
          SPDLOG_LOGGER_INFO(
              SpdLogger::Log(),
              "key(pos) < key(current): pos={} current={}, will MakeKey", pos,
              current);
          pos = MakeKey(key(pos) + 1, 0);  // 更新 pos
          // printf(
          //     "ReadStep: key(pos) < key(current) ,pos = %llx,current=%llx "
          //     "创建新key成功\n",
          //     pos, current);
          SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                             "key(pos) < key(current): pos={} current={}", pos,
                             current);

        } else {
          SPDLOG_LOGGER_INFO(
              SpdLogger::Log(),
              "key(pos) >= key(current): pos={} current={}, will MakeKey", pos,
              current);
          pos = MakeKey(key(pos), gen(pos) + 1);
          SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                             "key(pos) >= key(current): pos={} current={}", pos,
                             current);
        }

        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "while again");
      }
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "while end");

      if (!iter.Valid()) {
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "invalid iter");
        break;
      }

      /*
       * 经过一次[pos,current_) 的遍历后,如果current_后面还有值,继续遍历
       * 0.5的概率: 迭代器前进一位, pos 中的 gen +1
       *           在新的循环中如果迭代器无效,将 current_ 设置为最大值
       *           如果 current_ 有值,获取迭代器中的值
       * 0.5的概率: 获取新的 new_target,如果 new_target >pos ,
       *          更新 pos = new_target
       *          将 迭代器定位到跳跃表中 new_target 的下一个值
       * */
      if (rnd->Next() % 2) {
        // printf("0.5 概率迭代器前进 \n");
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "50% probability next");

        iter.Next();

        pos = MakeKey(key(pos), gen(pos) + 1);
        SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                           "50% probability: pos={} current={}", pos, current);
      } else {
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "50% probability random");

        Key new_target = RandomTarget(rnd);
        // printf(
        //     "0.5概率创建新的 target=%llx, pos=%llx 需要判断new_target 和 pos
        //     " "值\n", new_target, pos);
        SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                           "50% probability random new_target={},pos={}",
                           new_target, pos);

        if (new_target > pos) {
          // printf("0.5概率创建新的 target>pos");
          SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                             "new_target > pos, new_target={},pos={}",
                             new_target, pos);

          // printf("ReadStep: 旧 pos = %llx, 新 new_target=%llx\n", pos,
          //        new_target);

          pos = new_target;
          iter.Seek(new_target);
          // printf("ReadStep: 迭代器定位到 new_target=%llx\n", new_target);
        }
      }
    }
  }
};

// Needed when building in C++11 mode.
constexpr uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST(SkipTest, ConcurrentWithoutThreads) {
  ConcurrentTest test;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 100; i++) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"ReadStep {}",i);
    test.ReadStep(&rnd);
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),"WriteStep {}",i);
    test.WriteStep(&rnd);
  }
}

class TestState {
 public:
  ConcurrentTest t_;
  int seed_;
  std::atomic<bool> quit_flag_;

  enum ReaderState { STARTING, RUNNING, DONE };

  explicit TestState(int s)
      : seed_(s), quit_flag_(false), state_(STARTING), state_cv_(&mu_) {}

  // 该函数被调用到时候,本线程没有占有mu_锁
  void Wait(ReaderState s) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    while (state_ != s) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  // 该函数被调用到时候,本线程没有占有mu_锁
  void Change(ReaderState s) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    state_ = s;
    state_cv_.Signal();
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  ReaderState state_ GUARDED_BY(mu_);  // 表示 state_ 变量被 mu_ 保护
  port::CondVar state_cv_ GUARDED_BY(mu_);
};

static void ConcurrentReader(void* arg) {
  TestState* state = reinterpret_cast<TestState*>(arg);
  Random rnd(state->seed_);
  int64_t reads = 0;
  state->Change(TestState::RUNNING);
  while (!state->quit_flag_.load(std::memory_order_acquire)) {
    state->t_.ReadStep(&rnd);
    ++reads;
  }
  state->Change(TestState::DONE);
}

static void RunConcurrent(int run) {
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      std::fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestState state(seed + 1);
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int i = 0; i < kSize; i++) {
      state.t_.WriteStep(&rnd);
    }
    state.quit_flag_.store(true, std::memory_order_release);
    state.Wait(TestState::DONE);
  }
}

TEST(SkipTest, Concurrent1) { RunConcurrent(1); }
TEST(SkipTest, Concurrent2) { RunConcurrent(2); }
TEST(SkipTest, Concurrent3) { RunConcurrent(3); }
TEST(SkipTest, Concurrent4) { RunConcurrent(4); }
TEST(SkipTest, Concurrent5) { RunConcurrent(5); }

}  // namespace leveldb
