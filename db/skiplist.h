// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

template <typename Key, class Comparator>
class SkipList {
 private:
  // 声明仅告诉编译器存在一个名为Node的结构体类型，但不提供任何关于结构体成员的信息。
  //  因此，这样的声明本身不占用类SkipList的任何空间。
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

    /*
     * 内部类可以访问外部类的私有成员
     * 么node_的存储空间是在创建Iterator对象时分配的。即使Iterator是SkipList的内部类
     * node_也不会占用SkipList实例的直接存储空间，
     * 除非SkipList类自己也创建了一个Iterator实例并将其作为自己的成员变量存储。
     * */
   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };
  //  enum { kMaxHeight = 4 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // memory_order_acquire 用在load 时,保证同线程中该load之后的对相关内存读写
    // 语句不会被重排到 load 之前,并且其他线程中对同样内存用了 store release
    // 都对其可见

    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // memory_order_release 用在 store 时，保证同线程中该 store
    // 之后的对相关内存的读写 语句不会被重排到 store
    // 之前，并且该线程的所有修改对用了 load acquire 的其他线程都可见。

    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    // 下标引用和间接访问表达式一样
    next_[n].store(x,std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
    /* 这里时分配了指定的空间，没有创建对象
   * 创建一个新的结点
   * Node*存储指针
   * sizeof(Node) 用来保存 key 和一个 Node*
   * Node 中的 Node* 和 另外 height-1 个 Node* 一起构成 height 个Node*
   * */
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // placement new 在指定内存上创建对象
  return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 以 四分之一概率增加高度
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}


// 结点的 key 比要查找的key 小,说明 key 在 node 之后返回 true,
// 如果两者的user_key 相同，seq大的key 更小，排在更前面
// 如果相等返回false
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}
// 找到第一个大于等于 key 的 node.
// prev 保存的是每一层中比key小的结点中最大的节点
// 假设 skip_list 如下
// 1->2---->nullptr
// 1->2->5->nullptr
// 1->2->5->10
// 这里 1、2、5、10 分别代表一个节点
// 假设要查找 7,即 key=7
// 在 level = 2 查找,x=1,next=2, KeyIsAfterNode(7,2) = true
// 在本层查找, x=next=2 next=nullptr KeyIsAfterNode(7,nullptr) = false
// 跳到下一层 prev[2]=2 level=1
// 在 level = 1 查找, level =1,x=2, next=5 ,KeyIsAfterNode(7,5) = true
// 在本层查找, x=next=5,next=nullptr,KeyIsAfterNode(7,nullptr) = false
// 跳到下一层 prev[1]=5 level=1
// 在 level = 0 查找, level =0,x=5, next=10,KeyIsAfterNode(7,10) = false
// prev[0]=5 返回 next=10
// 最终返回 Node->key = 10,prev = [5,5,2]
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  // prev 是一个新的变量，指向 Node**
  Node* x = head_;                 // 从头结点开始查找
  int level = GetMaxHeight() - 1;  // 获取当前最高层,默认为1
  while (true) {
    Node* next = x->Next(level);  // 取出level 的结点
    if (KeyIsAfterNode(key, next)) {
      /*
       * 如果 user_key 相同，要查找的 key 的版本大于 next 则返回 false
       * 跳到下一层继续查找， next 的版本号大，则在同一层继续查找 Keep
       * searching in this list  这里查找的key相等(user_key+
       * 版本号)也会返回false 待查找 key 比 next 大, 则在该层继续查找
       * if(key>next)
       *    该层继续查找
       * else
       *    下一层查找
       * */
      x = next;
    } else {
      /*
       * 碰到 NULL 或者 key 比 next 小，则记录 next 的上一个节点
       * 将遍历过的结点记录在 prev 中
       * */
      if (prev != nullptr) prev[level] = x;
      // 已经找到最底层
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        // 没到底,继续查找
        level--;
      }
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    // 从最高层开始遍历 ,找不到 level -1
    Node* next = x->Next(level);
    // 结点的 key 比要查找的key 小,返回负数
    // 结点的 key 比要查找的key 大于或等于, 满足 if 条件
    // compare_(5,3) 返回 ture  满足条件时在当前结点的下一层继续查找

    // 0------------------------------------->nullptr
    // 0------------------------------------->nullptr
    // 0->3------------------------------>26->nullptr
    // 0->3->6->7->9->12->17->19->21->25->26->nullptr
    // 如果要查找的是  26 ,level =2
    // level = 3
    // level=3,x=0,next=nullptr => level--
    // level=2,x=0,next=nullptr => level--
    // level=1,x=0,next=3,compare(3,26)<0
    // level=1,x=3,next=26,compare(26,key)==0 这里已经找到了 26 .故这里有优化空间
    // 但 现在不能返回,level--
    // level=0,x=3,next=6,compare(6,key)<0
    // 继续遍历 6,7,9,12,17,19,21,25
    // 直到 x=25,next=26 才结束
    // 返回 x

    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 向右遍从上到下遍历跳表
// 每一层的 next 为空时，level -1
// 最后 next 指向空，返回 next 的前一个节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {  // 如果指向空,在该结点的下一层继续查找
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;  // 如果不等于空,则直接跳向下一个结点
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
        for (int i = 0; i < kMaxHeight; i++) {
          head_->SetNext(i, nullptr);  // 将每一个结点指针指向空
        }
      }

      template <typename Key, class Comparator>
      void SkipList<Key, Comparator>::Insert(const Key& key) {
        // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
        // here since Insert() is externally synchronized.
        // 构建一个数组,里面存查找的过程中走过的结点
        // 分配了内存，但未初始化，所以看得到的都是随机值
        Node* prev[kMaxHeight];
        Node* x = FindGreaterOrEqual(key, prev);
        // 返回一个比 key 相等或大于 key 的结点,并将这个 key
        // 插入这个结点之前

        // Our data structure does not allow duplicate insertion 不支持重复插入
        assert(x == nullptr || !Equal(key, x->key));

        // 随机设置一个新的高度值
        // 如果这个高度值大于当前的最高值
        //   (1) 将prev [GetMaxHeight(),height] 设置为 head_
        //   (2) 并更新 max_height_ 的值
        //
        int height = RandomHeight();
        if (height > GetMaxHeight()) {  // 获取跳表当前层数
          for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
          }
          // It is ok to mutate max_height_ without any synchronization
          // with concurrent readers.  A concurrent reader that observes
          // the new value of max_height_ will see either the old value of
          // new level pointers from head_ (nullptr), or a new value set in
          // the loop below.  In the former case the reader will
          // immediately drop to the next level since nullptr sorts after all
          // keys.  In the latter case the reader will use the new node.

          /*
           * 在并发读的情况下,要改变 max_height_ ,可以不加同步操作
           * 并发读的要么观察到新值 ???
           * store 内存序默认是  memory_order_seq_cst 最强的内存顺序约束
           * memory_order_relaxed 最宽松，所以效率最高
           */
          max_height_.store(height, std::memory_order_relaxed);
        }
        // 创建一个新的结点,height 是新建结点的高度,这里将新结点插入
        x = NewNode(key, height);
        // 插入 x 节,即 如果高度为 height, 每一层执行一个单链表的插入
        for (int i = 0; i < height; i++) {
          // NoBarrier_SetNext() suffices since we will add a barrier when
          // we publish a pointer to "x" in prev[i].
          // 基础的单链表插入
          // 最开始 prev-> prev_next
          // 插入 x : prev->x->prev_next
          x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
          prev[i]->SetNext(i, x);
        }
      }

      template <typename Key, class Comparator>
      bool SkipList<Key, Comparator>::Contains(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        if (x != nullptr && Equal(key, x->key)) {
          return true;
        } else {
          return false;
        }
      }

    }  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
