
#ifndef STORAGE_LEVELDB_INCLUDE_SPD_LOGGER_H
#define STORAGE_LEVELDB_INCLUDE_SPD_LOGGER_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/fmt/bin_to_hex.h"
//#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
namespace leveldb {
class SpdLogger {
 public:
  // static std::shared_ptr<spdlog::logger>& Log() {
  //   static std::shared_ptr<spdlog::logger> console_logger =
  //       spdlog::stdout_color_mt("console");
  //   console_logger->set_pattern("%m-%d %H:%M:%S.%e %s:%# %! [%t] %v");
  //   console_logger->set_level(spdlog::level::info);
  //   return console_logger;
  // }
  static std::shared_ptr<spdlog::logger>& Log() {
    static std::shared_ptr<spdlog::logger> console_logger = []() {
      auto logger = spdlog::stdout_color_mt("console");
      logger->set_pattern("%m-%d %H:%M:%S.%e %s:%# %! [%t] %v");
      logger->set_level(spdlog::level::info);
      return logger;
    }();  // 注意这里的 ()，表示立即执行 lambda
    return console_logger;
  }
};
}

#endif  // LEVELDB_SPD_LOGGER_H
