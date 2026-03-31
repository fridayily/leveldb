// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/histogram.h"

#include <cstdarg>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace leveldb{

TEST(Histogram,H){
  Histogram hist;
  hist.Clear();

  hist.Add(1.0);
  hist.Add(2.0);
  hist.Add(3.0);
  hist.Add(5.0);
  hist.Add(10.0);
  hist.Add(100.0);
  hist.Add(100.0);

  // 调用 ToString() 方法并打印结果
  std::string histogram_str = hist.ToString();
  std::cout << histogram_str << std::endl;
}

/*
 * va_list 是一个类型，用于表示可变参数列表。它实际上是一个指向参数栈的指针，用于遍历和访问函数的可变参数。
 * void va_start(va_list ap, last);
 *    ap：要初始化的 va_list 对象
 *    last：函数的最后一个固定参数（即可变参数前的那个参数）
 * void va_end(va_list ap);
 *    清理 va_list，释放相关资源，ap：要清理的 va_list 对象
 * void va_copy(va_list dest, va_list src);
 *    复制 va_list，用于多次处理同一个可变参数列表。
 *    dest：目标 va_list（将被复制到的对象）
 *    src：源 va_list（要复制的对象）
 * type va_arg(va_list ap, type);
 *    从 va_list 中提取下一个参数，并将 va_list 移动到下一个参数
 *    ap：va_list 对象
 *    type：要提取的参数类型
 * int vprintf(const char* format, va_list arg);
 *    format：格式化字符串，包含普通字符和格式说明符（如 %d、%s、%f 等）
 *    arg：va_list 类型的参数列表，由 va_start 初始化，用于提供格式化所需的参数
 */
// 辅助函数：使用 va_copy 复制可变参数列表
void TestVaCopy(const char* format, ...) {
  // 第一次使用可变参数
  va_list args1;
  va_start(args1, format);

  // 复制参数列表
  va_list args2;
  va_copy(args2, args1);

  // 使用第一个参数列表打印到标准输出
  printf("First use: ");
  vprintf(format, args1);
  printf("\n");

  // 使用复制的参数列表打印到标准输出
  printf("Second use: ");
  vprintf(format, args2);
  printf("\n");

  // 清理参数列表
  va_end(args1);
  va_end(args2);
}


void print_info(const char* name, int age) {
  char buffer[100];
  // 直接使用 snprintf，参数直接传递
  std::snprintf(buffer, sizeof(buffer), "Name: %s, Age: %d", name, age);
  std::printf("%s\n", buffer);
}

void log_message(const char* format, ...) {
  char buffer[100];
  va_list args;
  va_start(args, format);

  // 使用 vsnprintf 处理 va_list 参数
  std::vsnprintf(buffer, sizeof(buffer), format, args);

  std::printf("[LOG] %s\n", buffer);
  va_end(args);
}

// 辅助函数：接收 va_list 并处理
void ProcessVaList(const char* format, va_list args) {
  // 使用 vprintf 处理可变参数
  vprintf(format, args);
  printf("\n");
}

// 函数：接收可变参数并传递给 ProcessVaList
void PassVaListFunc(const char* format, ...) {
  va_list args;
  va_start(args, format);

  printf("Passing va_list: ");
  ProcessVaList(format, args);

  va_end(args);
}

// 函数：处理不同类型的可变参数
void ProcessDifferentTypes(const char* format, ...) {
  va_list args;
  va_start(args, format);

  // 手动解析不同类型的参数（仅作为示例，实际应用中应使用 vprintf 等函数）
  const char* fmt = format;
  while (*fmt) {
    if (*fmt == '%') {
      fmt++;
      switch (*fmt) {
        case 'd': {
          int value = va_arg(args, int);
          printf("Int: %d ", value);
          break;
        }
        case 's': {
          const char* value = va_arg(args, const char*);
          printf("String: %s ", value);
          break;
        }
        case 'f': {
          double value = va_arg(args, double);
          printf("Float: %f ", value);
          break;
        }
      }
    }
    fmt++;
  }
  printf("\n");

  va_end(args);
}

// 测试 va_copy 和 va_end 等可变参数函数的使用
TEST(VarArgsTest, VaCopyAndVaEnd) {
  // 测试不同类型的参数
  TestVaCopy("Integer: %d, String: %s, Float: %f", 42, "test", 3.14);
  TestVaCopy("Hello %s, you have %d messages", "user", 5);
}

// 测试可变参数在函数间传递
TEST(VarArgsTest, PassVarArgs) {
  print_info("Alice", 30);
  log_message("Application started");
  log_message("User %s logged in", "admin");
  log_message("Processing item %d of %d", 5, 10);
}

// 测试 va_list 的传递
TEST(VarArgsTest, PassVaList) {
  // 测试传递 va_list
  PassVaListFunc("Integer: %d, String: %s", 123, "test");
  PassVaListFunc("Float: %f, Double: %lf", 3.14f, 2.71828);
}

// 测试不同类型的可变参数
TEST(VarArgsTest, DifferentTypes) {
  // 测试不同类型的参数
  ProcessDifferentTypes("%d %s %f", 42, "hello", 3.14);
}

std::pair<std::string, int> TestVsnprintf(int buf_size, const char* format, ...) {
  // 使用 std::vector 替代变长数组，符合标准 C++
  std::vector<char> buffer(buf_size);
  va_list args;
  va_start(args, format);

  int result = std::vsnprintf(buffer.data(), buffer.size(), format, args);
  printf("Result: %d, Buffer: %s\n", result, buffer.data());
  va_end(args);
  
  // 返回格式化后的字符串和 vsnprintf 的返回值
  return std::make_pair(std::string(buffer.data()), result);
}


TEST(Vsnprintf,BufferSize) {
  // 测试 vsnprintf 函数
  auto result = TestVsnprintf(20,"Hello %s", "World");
  printf("Returned string: %s, vsnprintf result: %d\n", result.first.c_str(), result.second);
  ASSERT_EQ(result.first, "Hello World");
  ASSERT_EQ(result.second, 11); // "Hello World" 的长度是 11

  // 测试缓冲区不足的情况, 仍然返回理论占用字符数
  auto truncated_result = TestVsnprintf(5,"Hello %s", "World");
  ASSERT_EQ(truncated_result.first,"Hell");
  ASSERT_EQ(truncated_result.second,11);
}


}