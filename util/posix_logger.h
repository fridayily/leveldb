// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

class PosixLogger final : public Logger {
 public:
  // Creates a logger that writes to the given file.
  //
  // The PosixLogger instance takes ownership of the file handle.
  explicit PosixLogger(std::FILE* fp) : fp_(fp) { assert(fp != nullptr); }

  ~PosixLogger() override { std::fclose(fp_); }

  // 将数据格式化后写入文件，fp_在PosixLogger对象初始化时设置
  void Logv(const char* format, std::va_list arguments) override {
    // Record the time as close to the Logv() call as possible.
    struct ::timeval now_timeval;
    ::gettimeofday(&now_timeval, nullptr);
    const std::time_t now_seconds = now_timeval.tv_sec;
    struct std::tm now_components;
    // 将一个时间字符串或数字时间转换为本地时间的结构体
    ::localtime_r(&now_seconds, &now_components);

    // Record the thread ID.
    constexpr const int kMaxThreadIdSize = 32;
    std::ostringstream thread_stream;
    thread_stream << std::this_thread::get_id();
    std::string thread_id = thread_stream.str();
    if (thread_id.size() > kMaxThreadIdSize) {
      thread_id.resize(kMaxThreadIdSize);
    }

    // We first attempt to print into a stack-allocated buffer. If this attempt
    // fails, we make a second attempt with a dynamically allocated buffer.
    // 最开始尝试将要写入的数据格式化到一个栈分配的buffer中，如果失败，则尝试将数据格式化到一个动态分配的buffer中
    constexpr const int kStackBufferSize = 512;
    char stack_buffer[kStackBufferSize];
    static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize),
                  "sizeof(char) is expected to be 1 in C++");

    int dynamic_buffer_size = 0;  // Computed in the first iteration.
    for (int iteration = 0; iteration < 2; ++iteration) {
      const int buffer_size =
          (iteration == 0) ? kStackBufferSize : dynamic_buffer_size;
      char* const buffer =
          (iteration == 0) ? stack_buffer : new char[dynamic_buffer_size];
      // 获取buffer 和 buffer_size,用snprintf 写入 + 年月日时分秒微秒 + 线程id
      // Print the header into the buffer.
      int buffer_offset = std::snprintf(
          buffer, buffer_size, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
          now_components.tm_year + 1900, now_components.tm_mon + 1,
          now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
          now_components.tm_sec, static_cast<int>(now_timeval.tv_usec),
          thread_id.c_str());

      // The header can be at most 28 characters (10 date + 15 time +
      // 3 delimiters) plus the thread ID, which should fit comfortably into the
      // static buffer.
      assert(buffer_offset <= 28 + kMaxThreadIdSize);
      static_assert(28 + kMaxThreadIdSize < kStackBufferSize,
                    "stack-allocated buffer may not fit the message header");
      assert(buffer_offset < buffer_size);

      // Print the message into the buffer.
      std::va_list arguments_copy;
      // va_copy 函数的作用是将 src 变量的值复制到 dst 变量中。使用 va_copy 函数可以实现对可变参数列表的多次使用，
      // 避免了多次调用 va_start 和 va_end 函数的麻烦。
      va_copy(arguments_copy, arguments);
      // int snprintf(char *str, size_t size, const char *format, ...);
      // str 是目标缓冲区的指针，size 是缓冲区的大小，format 是格式化字符串，... 表示可变参数列表
      // int vsnprintf(char *str, size_t size, const char *format, va_list args);
      // args 是一个 va_list 类型的参数，通过 va_start 宏初始化，表示可变参数列表的起始位置。
      buffer_offset +=
          std::vsnprintf(buffer + buffer_offset, buffer_size - buffer_offset,
                         format, arguments_copy);
      va_end(arguments_copy);

      // The code below may append a newline at the end of the buffer, which
      // requires an extra character.
      if (buffer_offset >= buffer_size - 1) {
        // The message did not fit into the buffer.
        if (iteration == 0) {
          // Re-run the loop and use a dynamically-allocated buffer. The buffer
          // will be large enough for the log message, an extra newline and a
          // null terminator.
          dynamic_buffer_size = buffer_offset + 2;
          continue;
        }

        // The dynamically-allocated buffer was incorrectly sized. This should
        // not happen, assuming a correct implementation of std::(v)snprintf.
        // Fail in tests, recover by truncating the log message in production.
        assert(false);
        buffer_offset = buffer_size - 1;
      }

      // Add a newline if necessary.
      if (buffer[buffer_offset - 1] != '\n') {
        buffer[buffer_offset] = '\n';
        ++buffer_offset;
      }

      assert(buffer_offset <= buffer_size);
      // size_t fwrite(const void *ptr, size_t size, size_t count, FILE *stream);
      // ptr 指向写入的内存地址，size 要写入的每一个元素的大小，count 是写入的元素数量，stream 是指向文件指针的指针。
      std::fwrite(buffer, 1, buffer_offset, fp_);
      // 将输出流的缓冲区中数据立即写到其目标位置，而不是等待缓冲区满或则程序退出时才写入
      // 作用：在程序退出之前确保所有输出都已写入文件或终端
      //  在输出流被修改之后刷新流，以确保新的修改也立即写入目标位置
      std::fflush(fp_);

      if (iteration != 0) {
        delete[] buffer;
      }
      break;
    }
  }

 private:
  std::FILE* const fp_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
