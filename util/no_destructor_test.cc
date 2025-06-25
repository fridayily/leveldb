// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/no_destructor.h"

#include <cstdint>
#include <cstdlib>
#include <utility>

#include "gtest/gtest.h"

namespace leveldb {

namespace {

struct DoNotDestruct {
 public:
  DoNotDestruct(uint32_t a, uint64_t b) : a(a), b(b) {}
  ~DoNotDestruct() {
    std::abort();
  }  // std::abort()函数的作用是在不清理任何对象、不呼叫函数、不清空缓冲区的情况下,立即终止程序的执行

  // Used to check constructor argument forwarding.
  uint32_t a;
  uint64_t b;
};

constexpr const uint32_t kGoldenA = 0xdeadbeef;
constexpr const uint64_t kGoldenB = 0xaabbccddeeffaabb;

}  // namespace

TEST(NoDestructorTest, StackInstance) {
  NoDestructor<DoNotDestruct> instance(kGoldenA,
                                       kGoldenB);  // 类模板实例化   507b480
  ASSERT_EQ(kGoldenA, instance.get()->a);
  ASSERT_EQ(kGoldenB, instance.get()->b);
}

TEST(NoDestructorTest, StaticInstance) {
  static NoDestructor<DoNotDestruct> instance(kGoldenA, kGoldenB);
  ASSERT_EQ(kGoldenA, instance.get()->a);
  ASSERT_EQ(kGoldenB, instance.get()->b);
}

// 这里 union 没有实例化，没有提供直接访问联合体成员的途径
template <std::size_t _Len>
struct __aligned_storage_msa_self {
  union __type {
    unsigned char __data[_Len];
    struct __attribute__((__aligned__)) {
    } align;
  };
};

struct myStruct {
  union __union_type {
    int anInteger;
    float aFloat;
    char aCharacter;
  } __b;
};

class MyClass {
 private:
  int a;
  long b;

 public:
  MyClass() {
    a = 100;
    b = 1000L;
  }
  ~MyClass() { /* 析构逻辑 */ }
  // 其他成员和方法...
};



TEST(NoDestructorTest, AlignedStorage) {

  std::cout << "Alignment of int: " << alignof(int) << std::endl; // 输出 int 类型的对齐要求
  std::cout << "Alignment of double: " << alignof(double) << std::endl; // 输出 double 类型的对齐要求

  union MyUnion {
    int anInteger;
    float aFloat;
    char aCharacter;
  };
  // 创建一个实例，假设Len为10
  const std::size_t Len = 10;

  // 它用来告诉编译器__aligned_storage_msa_self<sizeof(MyClass)>::__type
  // 是一个类型名称，而不是一个非类型成员
  std::size_t _Align =
      __alignof__(typename __aligned_storage_msa_self<sizeof(MyClass)>::__type);

  std::cout << "_Align: " << _Align << std::endl;
  std::cout << "sizeof(MyClass): " << sizeof(MyClass) << std::endl;
  std::cout << "sizeof(MyUnion): " << sizeof(MyUnion) << std::endl;

  __aligned_storage_msa_self<sizeof(MyClass)> myAlignedClass;

  myStruct my_struct;
  my_struct.__b.anInteger = 100;
  std::cout << " sizeof(my_struct) " << sizeof(my_struct) << " bytes." << std::endl;

  std::cout << " sizeof(myAlignedClass) " << sizeof(myAlignedClass) << " bytes."
            << std::endl;

}

class Person {
 public:
  Person(std::string name, int age) : name_(name), age_(age) {
    std::cout << "Person constructed: " << name_ << ", " << age_ << std::endl;
  }

  std::string GetName() const { return name_; }
  int GetAge() const { return age_; }

 private:
  std::string name_;
  int age_;
};

TEST(NoDestructorTest, ConstructorArguments){
  // 使用 NoDestructor 实例化一个 Person 对象，传入两个参数
  NoDestructor<Person> personInstance("Alice", 30);

  // 获取 Person 实例并使用
  Person* personPtr = personInstance.get();
  std::cout << "Stored person: " << personPtr->GetName() << ", " << personPtr->GetAge() << std::endl;


}



}  // namespace leveldb
