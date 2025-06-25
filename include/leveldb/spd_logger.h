
#ifndef STORAGE_LEVELDB_INCLUDE_SPD_LOGGER_H
#define STORAGE_LEVELDB_INCLUDE_SPD_LOGGER_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
//#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
namespace leveldb {
class SpdLogger {
 public:
  static std::shared_ptr<spdlog::logger>& Log() {
    static std::shared_ptr<spdlog::logger> console_logger =
        spdlog::stdout_color_mt("console");
//    spdlog::set_pattern("[source %s] [function %!] [line %#] %v");
    console_logger->set_pattern("%m-%d %H:%M:%S.%e %s:%# %! [%t] %v");
    console_logger->set_level(spdlog::level::info);
    return console_logger;
  }
};
}

#endif  // LEVELDB_SPD_LOGGER_H
