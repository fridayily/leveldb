// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_
#define STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_

#include <type_traits>
#include <utility>

namespace leveldb {

// Wraps an instance whose destructor is never called.
//
// This is intended for use with function-level static variables.
template <typename InstanceType>
class NoDestructor {
 public:
  template <typename... ConstructorArgTypes> // 可变参数模板,允许不同类型
  explicit NoDestructor(ConstructorArgTypes&&... constructor_args) { // 构造函数 instance(kGoldenA, kGoldenB);
    static_assert(sizeof(instance_storage_) >= sizeof(InstanceType),
                  "instance_storage_ is not large enough to hold the instance");
    static_assert(
        alignof(decltype(instance_storage_)) >= alignof(InstanceType), // decltype 推断类型
        "instance_storage_ does not meet the instance's alignment requirement");
    // placement new 在已分配的特定内存创建对象  instance_storage_ 是已经分配了内存的变量
    // InstanceType 是一个类，在类中实现完美转发在指定的内存执行构造函数
    // forward 完美转发，若函数传入左值，转发到其他函数仍保留左值属性，如为右值，转发后仍为右值
    new (&instance_storage_)
        InstanceType(std::forward<ConstructorArgTypes>(constructor_args)...);
  }

  ~NoDestructor() = default; // 告诉编译器自动生成析构函数，即 trivial destructor

  NoDestructor(const NoDestructor&) = delete; // 禁止拷贝构造函数
  NoDestructor& operator=(const NoDestructor&) = delete; // 禁止赋值运算符

  InstanceType* get() {
    return reinterpret_cast<InstanceType*>(&instance_storage_); // 返回单例对象
  }
// aligned_storage 用于创建固定大小和对齐要求的未初始化内存，这里分配一个能存储InstanceType 对象的内存
// alignof 取结构体对齐长度
// 不会自动构造和析构对象

//  在C++中，std::aligned_storage<sizeof(InstanceType), alignof(InstanceType)>::type
//  是一个类型别名模板，它定义了一个未初始化的、足够存储InstanceType对象的联合体（或字节数组），
//  并确保该存储空间按照InstanceType的对齐要求进行对齐。
//  这里的sizeof(InstanceType)获取InstanceType类型的大小，而alignof(InstanceType)则是获取InstanceType类型的自然对齐值，
//  即编译器为其分配内存时所需的对齐边界。
//  因此，instance_storage_ 是一个变量，其类型由上述模板生成，
//  它的大小恰好可以容纳一个InstanceType实例，并且其地址满足InstanceType类型的对齐要求。
//  这种技术通常用于实现类型安全的无类型存储区域，例如在实现对象池、惰性初始化或其他需要延迟创建或销毁对象的场景中。
//  但请注意，虽然instance_storage_有足够的空间和正确的对齐来存储InstanceType对象，
//  但并没有实际创建InstanceType对象，若要构造或析构对象，还需要配合其他操作如placement new等。

 private:
  typename std::aligned_storage<sizeof(InstanceType),
                                alignof(InstanceType)>::type instance_storage_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_
