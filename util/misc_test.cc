// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/histogram.h"

#include <limits>
#include <string>

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

// 模拟日志函数
void LogMessage(const char* format, ...) {
  va_list args;
  va_start(args, format);

  // 打印日志前缀
  printf("[LOG] ");
  // 使用 vprintf 处理可变参数
  vprintf(format, args);
  printf("\n");

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
  // 测试日志函数
  LogMessage("Application started");
  LogMessage("User %s logged in", "admin");
  LogMessage("Processing item %d of %d", 5, 10);
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



}

