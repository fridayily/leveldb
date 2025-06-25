// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include <algorithm>
#include <cstdio>

#include "leveldb/env.h"
#include "leveldb/table_builder.h"

#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  // level0-1 都是10M 后面每升级一层，大小乘10
  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;  // 10
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

//  [1,3,5,7] 数字为文件中最大的 key 返回第一个大于等于 key 的索引
// 要查找的 key 为4
// left = 0,right = 4 ,mid =(0+4)/2=2
// files[mid].largest_key = 5
//      5 > 4
// right=2, left < right 继续循环，查找是否有比5更小的key
//     mid = (0+2)/2=1
//  files[mid].largest_key=3
//      3 < 4
//   left = 2 = right
//  返回 right 就是要找的比 target_key 大于等于的最小 key
// 在 files 查找第一个大于 key 的 largest_key 的索引
// 与每一个文件中的最大索引比较 ，Return the smallest index i such that
// files[i]->largest >= key.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {  // 二分查找
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];  // 如果中间的值比 key 大，则移动右边界
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) <
        0) {  //  判断(a - b)的值，a 是最大的key
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;  // 返回的是一个索引
}
// AfterFile 传入的 key 与文件中最大值比较
// 如果要查找的 user_key 在指定文件 之后，返回 true
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}
// beforeFile 传入的key与 文件中最小值比较
// 指定的user_key 小于 f 中的最小key
// 如果要查找的 user_key 在指定文件 之前，返回 true
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  // 传入的 user_key 与文件中最小的 key 的 user_key 比较
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  // user_key 的比较器
  const Comparator* ucmp = icmp.user_comparator();
  // 如果不是不相交的有序文件，顺序遍历文件
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key,
                     f)) {  // 两个都为false，函数返回true
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }
  // 如果是相交有序文件，则进行二分查找
  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    // 在文件数组中找最小的largest_key的索引，该largest_key >=small_key
    index = FindFile(icmp, files, small_key.Encode());
  }
  // 如果files为空，返回 index为0
  if (index >=
      files.size()) {  // FindFile 查找的索引超出文件数，返回False,说明无重叠
    // beginning of range is after all files, so no overlap.
    // 范围的开始在所有文件之后
    return false;
  }
  // 如果查找的key在 files[index] 之前，BeforeFile 返回True
  // <150,200>  <200,250>  <300,350>
  // 现在要查找 user_key_range <251,260>
  // 先用 最小user_key 251 进行 FindFile(251) 返回 index=2
  // 然后 最大user_key 260 与 <300,350>的最小值300 比较
  // 即 BeforeFile(ucmp,260,files[2]) 来判断 260 是否在 files[2] 之前
  // 返回 true
  // 综上，files[2]的最大key 大于等于
  // 最小的user_key，即其余文件的最大key都小于该 user_key 因此需要取 files[2]
  // 的最小key 判断是否可能与 user_key_range 相交 user_key_range.largest_key <
  // files[2].smallest 因此无相交，最终返回false
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {  // parsed_key.user_key 迭代器中的key, s->user_key 要查找的key
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state =
          (parsed_key.type == kTypeValue)
              ? kFound
              : kDeleted;  // 如果查找的key是插入的返回found,如果是delete的，返回删除
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

// user_key 和 internal_key 的 user 部分相等
// 对于 level0 ,直接逐个判断里面的f的 smallest 和 largest 是否包含 user_key
// 如果包含，先放到临时的 vector 中，然后排序，然后逐个调用 func 函数
// 对于大于 1 的level, 先找到第一个file.largest 大于 internal_key 的 file
// 如果该  file.smallest 小于 user_key，说明有重叠，调用 func 函数
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());  // level 0 的文件个数
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;
    // 二分查找在指定的level 中查找第一个 largest key >= internal_key
    // 的文件索引，找不到返回0 进一步，如果 user_key 小于 上面查找的f.smallest,
    // 说明没有重叠，继续循环判断 如果 user_key
    // 大于等于上面查找的f.smallest,说明有重叠，则调用 f 处理 Binary search to
    // find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key 要查找的key 比 返回的 f
        // 的最小key还小，即不再范围内
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}
// 将查找的key 放入 value, 并填充 stats
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;
  // 定义一个结构体用于保存在迭代过程中需要的变量
  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;
    // 类中定义的静态函数，
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {  // 这次查找超过一次
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }
      // 之前没有seek 过，即第一次seek
      state->last_file_read = f;
      state->last_file_read_level = level;
      // 先在缓存中查找key,缓存中没有再在文件中查找，并将文件(ldb文件id和
      // 存储key的 index_block)存到缓存
      state->s = state->vset->table_cache_->Get(
          *state->options, f->number, f->file_size, state->ikey, &state->saver,
          SaveValue);  // SaveValue 是一个回调函数，用于保存查找的结果
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;  // 初始化，默认未找到
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();  // 从 lookupkey  中抽取 internal_key,即
                                  // user_key + type + sequence
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();  // 要查找的user_key
  state.saver.value = value;            // 查找后的值存入value
  // 遍历 current 中的每一层的 file，对于每一个和 user_key 有重叠的 file,调用
  // Match 函数, file 结构体中有cache_id 或 ldb
  // 文件id,可用在缓存中或磁盘文件中查找对应的key
  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);
  // 调用 Match 函数 将根据 key 查找得到的 value 放入state.saver 中
  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

// 记录读取的样本
bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  // 将internal_key 拆分成 ParsedInternalKey
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file 保存第一次match 的文件元信息
    int matches;     // match 的数量

    // Match 函数返回 False 时,ForEachOverlapping 结束
    static bool Match(void* arg, int level, FileMetaData* f) {
      // 强制类型转换
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match. 记录第一次match的文件元信息
        state->stats.seek_file = f;  // 这里记录的文件包含 internal_key
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      // 匹配到2个时，Match 返回 False, ForEachOverlapping
      // 结束，这时只记录了第一次match的文件元信息
      return state->matches < 2;
    }
  };

  State state;  // 内部类的初始化
  state.matches = 0;
  // 对于每一个重叠的key,调用 Match 函数
  // internal_key 是 user_key + type + sequence ，ikey 是拆分好的 internal_key
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  // 必须至少有两个matches,因为我们需要合并多个文件，
  // 但是一个文件中包含多个重叠的key，有可能只匹配一次，此时不会触发 compaction

  // 如果只 match 一次，不会触发 compaction,2 次时有可能
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}
// level>0 说明是已排序文件，即 disjoint_sorted_files = true, level=0
// 是未排序文件
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}
// 决定SST table 位于那一层
// 设置 level=0
// 如果第0层没有重叠，继续判断 （最多两次循环）
//    (1) 如果在第level+1层中有重叠，====> 跳出循环 返回 level
//    (2) 如果在第level+1层中没有重叠
//   （3）获取的 level +2 层中与待插入key重叠的文件
//   （4）获取这些文件的大小
//   （5）如果计算的文件大小大于20M ====> 跳出循环 返回 level
//    (6) 如果小于20M, level+1, 跳到第（1）步
// 如果第0层有重叠，则level = 0
//

// 确定 memtable 的输出应该位于哪一层
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level =
      0;  // 有重叠返回true，无重叠返回false,说明有重叠，设置为level0,否则进一步判断
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,  key 没有重叠
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      // 如果在下一层级上存在重叠，跳出循环，即在第1层有重叠，设置level=0,第2层有重叠，设置level=1
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;  // 返回 true 说明有重叠
      }
      // 如果存在下下层级
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(
            level + 2, &start, &limit,
            &overlaps);  // 将与[start,limit] 有重合的f放入overlaps中
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}
// 将第level 层文件中与[begin,end]有重合的 file 放入 inputs 中
// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it f
      // 的范围在检查范围之外，跳过检查 f
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        //   user_begin > f.smallest 之前，重新设置要比较的开始位置
        //  f1:<10,20> f2:<15,30> 要比较 <5,15>
        //         第一轮: <5，15>和 f1比较  10 < 5 为 false , 20> 15 为true ->
        //         设置 user_end = 20,重新遍历 第二轮: <5,20> 和 f1比较  10< 5
        //         为 false, 20>20 为 false  inputs 里面有f1 第三轮: <5,20> 和
        //         f2比较 15< 5 为 false,30>20 为 true-> 设置 user_end=30
        //         ,重新遍历 第四轮：<5,30> 和 f2比较 15< 5 为 false,30>30 为
        //         false inputs 里面有f1,f2
        //
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;  // 重新设置开始位置
          inputs->clear();
          i = 0;  // 重新开始搜索
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          // user_end < f.largest 重新设置结束位置
          user_end = file_limit;
          inputs->clear();
          i = 0;  // 重新开始搜索
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}
// VersionSet 的一个内部类，edit要添加数据到 VersionSet
// 时，要先更新该类成员变量数据
// A helper class so we can efficiently apply a
// whole sequence of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
// Version 保存的是各 level 下每个 sstable 的 FileMeta,
// 如 filenumber、filesize,smallestkey,largestkey
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };
  // set<key,比较器>
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    // 每一层都有一个FileSet ,可以保存每一层每一个文件的元信息
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  // 每层的状态，即要添加的文件和要删除的文件
  LevelState levels_[config::kNumLevels];

 public:
  // 每次构造函数执行一次，向每一层的level中添加Fileset
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "version set build ");
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;  // 设置内部比较器
    for (int level = 0; level < config::kNumLevels; level++) {
      // 自定义比较器,set 的初始化
      // 一个 builder 中包含 7 层文件
      // 每一层都包含 deleted_files 和 added_files
      // deleted_files 保存文件id
      // added_files 是需要 add 的文件,元素是文件的元信息,按照 smallest key 排序
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      // 获取指定层级的 FileSet，存的是 FileMetaData 指针
      // 该 FileSet 中的 FileMetaData 可能被多个地方引用
      // 这里只删除 Builder 中的引用
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      // added_files 是 new 出来的
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        // 如果f 的引用计数大于0，则不会删除
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  // edit 记录的是编辑相关操作，即 compact delete add_new_file
  // 即将变化记录在 build 中
  void Apply(const VersionEdit* edit) {
    // Update compaction pointers
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "update compact pointer of vset_");
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "update deleted files of vset_");
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "update new files of vset_");
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level =
          edit->new_files_[i].first;  // 存的是 <level,FileMetaData>
      // 取出 filemeta
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;
      // 当查找该文件的次数达到一定次数时，会触发自动压缩
      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // 每 25 次的查找成本相当于压缩 1MB 数据的成本，即一次查找的成本大约等于
      // 压缩 40KB 的数据成本
      // 我们有点保守，在触发压缩之前，大约允许对每16KB的数据进行一次查找
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));  // 16KB
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      // 从删除文件集合中移除待添加文件
      levels_[level].deleted_files.erase(f->number);
      // 查找这个 f 达到 allowed_seeks 后触发压缩
      levels_[level].added_files->insert(f);
    }
  }
  // std::upper_bound 返回第一个大于给定值的元素的迭代器
  // Save the current state in *v.  将旧数据，加上需要添加的数据重新写到v 中
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    // 这里是三级 for 循环
    // 第一级for针对每一层，即对每一 level 进行相同处理
    // 第二级for处理 added_files 中每一个 added_file, added_files 是一个数组
    // 第三级for 将小于等于added_file 的 base_file 添加到 v 中
    //  针对每一层，存在 base_ 预先存在的文件， added_file 需要添加的文件
    //    base_ <1,2,3,5,6,7,11,12>    added_file <5,10>
    //    第一次循环，从 added_file 取出 5，将 base_ 中小于5 的添加到 v 中,即
    //    1，2，3 第二次循环，从 added_file 取出 10，从 5开始，将 base_ 中小于
    //    10 的添加到 v 中
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files =
          base_->files_[level];  // base_即current，获取之前存在的文件
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      // 需要添加的文件
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());
      // 合并添加的文件和已存在的文件
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        // 将所有小于 added_file 的元素添加到v中

        // std::upper_bound 用于在已排序的范围内查找第一个大于给定值的元素的位置
        // 这里在 base_files 中查找第一个大于等于 added_file 的元素的位置
        // 若找不到返回last

        // 这里先初始化 bpos,即第一个大于 added_file 的元素的迭代器,作为 for
        // 停止条件 最终就是将小于等于 add_file 的元素添加到v

        // 注意这里 base_iter 是 base_的迭代器指针，多次for
        // 循环，该指针是接着上次在+1
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);  // 这里更新
        }
        // 将added_file添加到 edit 中
        MaybeAddFile(v, level, added_file);
        // 最终将小于 added_file 的元素和 added_file 添加到 v 中
      }

      // Add remaining base files
      // 将剩余的 base_files 添加到 v 中
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
      // 最终将旧的 file 和 新的 file 添加到 v->files_,而且保证 file
      // 从小到大排列

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          // 前一个文件的最大key
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          // 后一个文件的最小key
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          // 在同一层中有多个file, 一般按顺序存储
          // 如果前一个文件的最大键大于等于后一个文件的最小键，则说明有重叠
          // 则输出错误信息并退出程序
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }
  // 将filemeta 添加到指定 level 的vector<FileMetaData*>中
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      // 获取指定 level 的所有文件
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);  // 修改的是 v->files
    }
  }
};

// 当创建一个对象时，编译器首先为该对象分配足够的内存空间。
// 这个内存空间的地址就是 this 指针所指向的地址
// 所以在初始化列表中可以使用 this
// 注意这里 dummy_versions_ 是 Version 类
// 这里构造了一个空的 Version
// 在构造函数的函数体中会新建一个 Version,添加到 dummy_version 尾部
// VersionSet 整个程序生命周期只会实例化一次
// dummy_versions_ 就是实例化后的地址,始终保持不变
VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "VersionSet Construct");
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  // 析构函数也不会修改 options_ 和 table_cache_
  // 因为是 const 的,这两变量可有其他类修改
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  // 初始化时先 descriptor_file_  再  descriptor_log_
  // 析构时先 descriptor_log_ 再 descriptor_file_
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    // current 不为空时，说明已经指向某个 Version
    // 现在要修改 current 的指向，将现在指向的 Version 引用减一
    current_->Unref();
  }
  // current 指向待插入的 Version
  current_ = v;
  v->Ref();

  // Append to linked list
  // 将当前的 version 插入循环双向链表的尾部
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// current_version + edit 生成新的 version ,添加到双向循环链表
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  // 下一个要用的文件序号
  edit->SetNextFile(next_file_number_);
  // 最新使用的文件序号,一般为已写的key 的数量
  edit->SetLastSequence(last_sequence_);
  // 新的版本，将文件变动存入其中
  Version* v = new Version(this);
  {
    // 每次构造函数执行，每层的added_files都添加一个FileSet
    Builder builder(this, current_);
    // 将 edit 数据记录到 builder
    builder.Apply(edit);
    // 将修改保存到新的Version ,将 edit 与 current_ 合并
    builder.SaveTo(v);
  }
  // 确定下次 compaction 时选择那一层
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // 如果为空，说明需要新建Manifest文件
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndALogAndApplypply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();
    // 释放锁，允许其他 BackgroundCall 执行
    // Write new record to MANIFEST log  写一个record 到 MANIFEST
    if (s.ok()) {
      std::string record;
      // 将 edit 信息编码为 record，
      // 即将文件修改信息（添加、删除文件的元信息）写到 MANIFEST 文件中
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);  // 添加 record 到 MANIFEST 文件中
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    // 将 version 添加到 version_set 双向链表中
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool* save_manifest) {
  // 用于记录 Log 中的异常信息
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  // 将 CURRENT 文件中的内容读到 current
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);  // 去掉最后一个字符 \n
  // DescriptorFile
  // 从CURRENT 文件中确定 MANIFEST 文件
  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create descriptor file {}", dscname);
  // 创建一个读取 MANIFEST 的对象
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "version builder in Recover");
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;  // 创建MANIFEST文件的状态
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "{} log reader construct", dscname);
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      SPDLOG_LOGGER_INFO(SpdLogger::Log(), "read {} record", read_records);
      ++read_records;
      VersionEdit edit;
      // MANIFEST 文件配置读到 record,record 解码到 edit 中
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        SPDLOG_LOGGER_INFO(SpdLogger::Log(), "apply edit in Recover");
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Finalize");
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}
// 设置一个版本的最佳压缩级别
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      // 特殊处理 level0 的情况，使用文件数量而不是字节数来计算得分
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      // 具有较大的写入缓冲区大小时避免过多的Level-0压缩
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      // 每次读操作都会merge
      // level-0的文件，由于单个文件较小时，我们希望避免太多的小文件产生 (2) The
      // files in level-0 are merged on every read and therefore we wish to
      // avoid too many files when the individual file size is small (perhaps
      // because of a small write-buffer setting, or very high compression
      // ratios, or lots of overwrites/deletions).
      score =
          v->files_[level].size() /
          static_cast<double>(
              config::
                  kL0_CompactionTrigger);  // 第level层的文件数量/kL0_CompactionTrigger
    } else {
      // Compute the ratio of current size to size limit.
      // 对于非Level-0的级别，计算当前级别的字节数与该级别的最大字节数限制的比率来得到得分
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                       "level {} score {} best_level {} best_score {}", level,
                       score, best_level, best_score);
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
    SPDLOG_LOGGER_INFO(SpdLogger::Log(),
                       "level {} score {} best_level {} best_score {}", level,
                       score, best_level, best_score);
  }
  // level 0 根据文件数量，level1-5 根据文件大小
  v->compaction_level_ = best_level;  // 当前7层中最应该 compaction 的 level
  v->compaction_score_ = best_score;  // 当前7层中最应该 compaction 的分数
  SPDLOG_LOGGER_INFO(SpdLogger::Log(), "best_level {} best_score {}",
                     best_level, best_score);
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}
// 获取 level 的结点数量
int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}
// for 循环起始为 current, 结束为 dummy_versions_,这是一个双向循环链表，
// 遍历整个 version_set 链表，一个Version 中包含每层的文件，将每层的file
// 添加到live中
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);  // 将每一层的文件编号添加到live中
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();  // inputs 中有多个文件，可能 inputs[0].smallest=
                     // inputs[1].smallest inputs[0].largest= inputs[1].largest
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(),
             inputs2.end());  // 向all后面添加整个inputs2
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.  查找 vector<file>  中最大的InternalKey
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;            // 取 0号索引
  for (size_t i = 1; i < files.size(); ++i) {  // 取1 号索引
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}
// 设 files[1] b1=(l1,u1) files[2] b2=(l2,u2), user_key(l2) = user_key(u1), l2>
// u1 ,即 l2的 sequence 小于 u1 ,u1 更新 Finds minimum file b2=(l2, u2) in level
// file for which l2 > u1 and user_key(l2) = user_key(u1) 指定一个 largest_key
// 遍历 level_files 中每一个 file 的 smallest_key
// 返回 smallest_key 中最小且比 largest_key 大的 元文件
// 即 min(smallest_key0,smallest_key1,...,smallest_key_n) 且 min_smallest_key>
// largest_key 且 user_key(min_smallest_key)=user_key(largest_key) 注意
// internalKey 的比较方式是: user_key 是正序，sequence_num 是倒序
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    // 进入if的条件是 user_key 相等，其f->smallest 比 largest_key 更新，进入if
    // 才能修改 smallest_boundary_file
    if (icmp.Compare(f->smallest, largest_key) > 0 &&  // 比较的是 InternalKey
        user_cmp->Compare(
            f->smallest.user_key(),
            largest_key.user_key()) ==  // 比较的是InternalKey 之中的user_key
            0) {
      if (smallest_boundary_file == nullptr ||  // 第一次进来为空
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) <
              0) {  // 第n次进入，选最更小的文件
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// 从 |compaction_files| 中提取最大的文件 b1。
// 在 |level_files| 中查找一个文件 b2，使得 user_key(u1) = user_key(l2)，
// 其中 u1 是 b1 的结束键，l2 是 b2 的起始键
// 如果找到了这样的文件 b2（称为边界文件），则将 b2 添加到 |compaction_files| 中
// 使用新的上界 u2 继续搜索，重复上述过程。
// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// 假设有两个 block,b1的上界和b2的下界的user_key 相等
// 如果对 b1 进行压缩（compaction），但不对 b2
// 进行压缩，那么后续的查询操作可能会返回 错误的结果 由于 b2
// 没有被压缩，查询操作可能会在第 i 层找到并返回 b2 中的记录，而不是 b1 中的记录
// 查询操作通常会从最上层开始逐层向下搜索，直到找到匹配的记录
// 如果 b1 被压缩而 b2 没有被压缩，那么 b1 中的数据可能已经被移动到更低的层，
// 而 b2 仍然保留在较高的层
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
// 在 compaction_files 中确定 largest_key
// 用 largest_key 去和 level_files 的每一个 smallest_key 比较
// 将符合条件的文件添加到 compaction_files 中
// https://www.calvinneo.com/2021/04/18/leveldb-compaction/
// https://github.com/google/leveldb/pull/339
// https://github.com/google/leveldb/issues/320
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  // 从 compaction_files 中找出最大的 key
  if (!FindLargestKey(
          icmp, *compaction_files,
          &largest_key)) {  // 这里从compaction_files 获取最大的InternalKey,存到
                            // largest_key 中
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    // 根据 largest_key 从 level_files 找出最小的 BoundaryFile
    // BoundaryFile 的定义
    // 从 compaction_files 找出一个文件 b1(l1,u1)
    //        条件是 u1 是 compaction_files 中 largest_key 最大的
    // 从 level_files 找出 b2(l2,u2)
    //        条件是  (1) user_key(u1)=user_key(l2)
    //               (2) l2 > u1
    //               (3) l2 是 level_files 中 smallest_key 最小的
    // 则 b2 被称为 BoundaryFile
    // 找到后添加到 compaction_files，更新 largest_key ,在继续查找
    // 看 TestDisjoinFilePointers
    // 第一次的 largest_key 来源于 compaction_files
    // 之后会更新 largest_key，来源于 level_files
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;  // 更新 largest_key
    } else {
      continue_searching = false;
    }
  }
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);// 将level层与begin,end重合的文件放在 inputs 中
  if (inputs.empty()) { // 没有重合的文件，直接返回
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
