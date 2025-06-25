// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {
// 将Sequence 和 Type 打包
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek); // 7字节
  return (seq << 8) | t;  // 加起来8字节
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size()); // 将实际数据存入result
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type)); // 在真实数据后面添加sequence和type
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)  user key 升序
  //    decreasing sequence number   sequence num 降序，sequence number 越大值越新
  //    decreasing type (though sequence# should be enough to disambiguate)  type 降序，值大表示删除
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) { // 先比较 sequence number + type 形成的数字，虽然只比较 sequence 就足够了
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8); // 取出 sequence number 和 type
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) { // anum>bnum 表示是a比b 更新
      r = -1;
    } else if (anum < bnum) { //  b 比 a 更新
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() && // 如果相比较的字符串相等，tmp 和 user_start 大小相等
      user_comparator_->Compare(user_start, tmp) < 0) { //user_start 字符串小于 tmp
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp); // 交换字符串,地址是不变的
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0); // 上一个存入的key 比 tmp 小
    key->swap(tmp); // key 和 tmp 交换
  }
}

const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]); // 将internal_key 转为 user_key, 还是原地址
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst); // 将滤波器的结果存入dst中
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}
//  根据user_key 组装成 lookup key，s指定版本
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate 保守估计
  char* dst;
  if (needed <= sizeof(space_)) {  // 如果预估的key长度小于200，则将dst指向结构体中已分配的空间
    dst = space_;
  } else {
    dst = new char[needed]; // 否则分配需要的空间
  }
  start_ = dst; // 记录起始位置
  dst = EncodeVarint32(dst, usize + 8); // 将计算的 internal_key_size(user_key + value_type + sequenceNum) 存入dst
  kstart_ = dst; // 记录 internal_key 的开始位置
  std::memcpy(dst, user_key.data(), usize); // 将user_key 存入 dst
  dst += usize; // 指针偏移
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek)); // 将打包好的 sequence + type 存入 dst
  dst += 8; // 指针偏移
  end_ = dst; // 存入  internal_key 结束位置
}

}  // namespace leveldb
