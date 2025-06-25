// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#ifndef __Fuchsia__
#include <sys/resource.h>
#endif
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
// 常量表达式，在编译期确定其值
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

// 小于这个值的会写入缓存中，大于这个值的直接写到文件
constexpr const size_t kWritableFileBufferSize = 65536;  // 64*1024

Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  Limiter(int max_acquires)
      :
#if !defined(NDEBUG)
        max_acquires_(max_acquires),
#endif  // !defined(NDEBUG)
        acquires_allowed_(max_acquires) {
    assert(max_acquires >= 0);
  }

  Limiter(const Limiter&) = delete;            // 拷贝构造函数
  Limiter operator=(const Limiter&) = delete;  // 禁止赋值操作符重载

  // If another resource is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);

    if (old_acquires_allowed > 0) return true;  // 如果还有可用资源，返回 true

    int pre_increment_acquires_allowed =  // 说明没有可用资源，先加1个可用资源，但是会返回
                                          // false
        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);

    // Silence compiler warnings about unused arguments when NDEBUG is defined.
    (void)pre_increment_acquires_allowed;
    // If the check below fails, Release() was called more times than acquire.
    assert(pre_increment_acquires_allowed < max_acquires_);

    return false;
  }

  // Release a resource acquired by a previous call to Acquire() that returned
  // true.
  void Release() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);

    // Silence compiler warnings about unused arguments when NDEBUG is defined.
    (void)old_acquires_allowed;
    // If the check below fails, Release() was called more times than acquire.
    assert(old_acquires_allowed < max_acquires_);
  }

 private:
#if !defined(NDEBUG)
  // Catches an excessive number of Release() calls.
  const int max_acquires_;
#endif  // !defined(NDEBUG)

  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.  可移植操作系统接口
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(std::move(filename)) {}
  ~PosixSequentialFile() override { close(fd_); }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size =
          ::read(fd_, scratch, n);  // 尝试读取n个字节到scratch
      if (read_size < 0) {          // Read error.
        // 通过read读数据，当前fd对应的缓冲区没有数据可读时，进程被阻塞，
        // 此时如果向该进程发送信号，read函数返回-1，errno=EINTR
        if (errno ==EINTR) {
          continue;   // Retry
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status Skip(uint64_t n) override {
    // 从 SEEK_CUR 将文件指针偏移 n 个字节，返回新的文件指针位置
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
      : has_permanent_fd_(fd_limiter->Acquire()),  // 能打开的文件描述符减1
        fd_(has_permanent_fd_
                ? fd
                : -1),  // 有可用文件描述符资源，则赋值为fd,否则 -1
        fd_limiter_(fd_limiter),
        filename_(std::move(filename)) {
    if (!has_permanent_fd_) {
      assert(fd_ == -1);
      ::close(fd);  // The file will be opened on every read.
    }
  }

  ~PosixRandomAccessFile() override {
    if (has_permanent_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      fd_limiter_->Release();  // 能打开的文件描述符数量加1
    }
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    int fd = fd_;
    if (!has_permanent_fd_) {
      fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
      if (fd < 0) {
        return PosixError(filename_, errno);
      }
    }

    assert(fd != -1);

    Status status;
    // fd 打开的文件描述符，buf 用于存储数据的缓冲区，count 要读取的字节数
    // offset 要读取的位置，以偏移量表示 read 函数是从文件当前位置读取数据 pread
    // 从指定位置读取数据
    ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
    if (read_size < 0) {
      // An error: return a non-ok status.
      status = PosixError(filename_, errno);
    }
    if (!has_permanent_fd_) {
      // Close the temporary file descriptor opened earlier.
      assert(fd != fd_);
      ::close(fd);
    }
    return status;
  }

 private:
  const bool has_permanent_fd_;  // If false, the file is opened on every read.
  const int fd_;                 // -1 if has_permanent_fd_ is false.
  Limiter* const fd_limiter_;
  const std::string filename_;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // acquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                        Limiter* mmap_limiter)
      : mmap_base_(mmap_base),
        length_(length),
        mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
    mmap_limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > length_) {  // 大于文件总长度，则报错
      *result = Slice();
      // EINVAL linux系统中返回值为负数的错误码，比哦啊是传入参数无效
      return PosixError(filename_, EINVAL);
    }
    // mmap_base_ 是一个指向打开文件的内存地址，offset
    // 偏移量，返回的是一个指针，没有额外占用内存
    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_;
  const size_t length_;
  Limiter* const mmap_limiter_;
  const std::string filename_;
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }
  /* 将数据添加到 pos_中 ，只要没有超过缓冲区，可一直append
   * 超过缓存区后写到文件中
   * */
  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    // min(应该要写的长度,剩余缓冲区长度)
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);  // buf_是buf开始的地址
    write_data += copy_size;  // write_data 的指针移动,可能只写一部分
    write_size -= copy_size;
    pos_ += copy_size;  // 写指针偏移
    if (write_size == 0) {
      // 全部写完,说明缓冲区足够容纳本次要写的数据
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    // 写的数据较多，缓存区写满，还有数据要写如，先将缓存区的数据写到文件
    // 写完后会将偏移量置为 0
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "write buffer data to file");
    Status status = FlushBuffer();  // 将在缓冲区的数据写到文件中
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    // 待写的数据小于缓冲区长度，则写到缓冲区中，否则直接写到文件中
    if (write_size < kWritableFileBufferSize) {
      // 写到缓冲区中
      SPDLOG_LOGGER_INFO(
          SpdLogger::Log(),
          "remain bytes less then kWritableFileBufferSize, write to file");
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    SPDLOG_LOGGER_INFO(
        SpdLogger::Log(),
        "remain bytes larger then kWritableFileBufferSize, write to file");
    return WriteUnbuffered(write_data, write_size);  // 写到文件中
  }

  Status Close() override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Close file, FlushBuffer");
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Flush() override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "Flush");
    return FlushBuffer();
  }

  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    return SyncFd(fd_, filename_);
  }

 private:
  Status FlushBuffer() {
    // writable_file->Append 中更新了 buf_ pos_
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {  // 遇到中断返回-1,errno=EINTR
          continue;            // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;  // 移动指针,不是加长度
      size -= write_result;
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  // 确保与给定文件描述符的数据相关联的所有缓存一直刷新到持久介质，并且可以承受电源故障。
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
    // On macOS and iOS, fsync() doesn't guarantee durability past power
    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
    // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
    // fsync().
    // 用于同步文件系统的数据，它会等待所有数据都被写到磁盘，确保文件系统完整性
    // ，F_FULLFSYNC会将数据写入到磁盘的每个块，并确保每个块都写入完成。
    // 而fsync只是将数据写入到操作系统的缓存中，由操作系统决定何时将数据写入到磁盘
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
      return Status::OK();
    }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
    bool sync_success = ::fdatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

  // Returns the directory name in a path pointing to a file.
  // 返回文件夹名
  // Returns "." if the path does not contain any directory separator.
  static std::string Dirname(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  static Slice Basename(const std::string& filename) {
    std::string::size_type separator_pos =
        filename.rfind('/');  // 逆向查找'/'，返回第一个碰到的索引位置
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) ==
           std::string::
               npos);  // 从 指定索引位置开始查找，之后是文件名，应该不存在'/'

    return Slice(filename.data() + separator_pos + 1,     // 文件名的开始位置
                 filename.length() - separator_pos - 1);  // 文件名的长度
  }

  // True if the given file is a manifest file. 清单文件
  static bool IsManifest(const std::string& filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  char buf_[kWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_;  // The directory of filename_.
};

// 传入非0值上锁
int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;  // linux 文件锁
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);  // l_type 指定锁的类型，
  // SEEK_SET 用于指定文件指针的移动方式，具体来说，它表示从文件的开头开始计算偏移量
  file_lock_info.l_whence = SEEK_SET;  // l_whence,l_start,l_len 共同设置锁定区域
  file_lock_info.l_start = 0; // 起始偏移量
  // l_len 锁定区域的长度,0 表示到文件末尾
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  // F_SETLK 是 fcntl 系统调用中的一个命令，用于设置文件锁。它允许进程请求或释放文件上的锁，
  // 并且会立即返回，如果请求的锁无法立即获得，则会返回错误。
  return ::fcntl(fd, F_SETLK, &file_lock_info);
  // https://pubs.opengroup.org/onlinepubs/007904975/functions/fcntl.html
  // Set or clear a file segment lock according to the lock description pointed
  // to by the third argument
}

// Instances are thread-safe because they are immutable.
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}  // 移动指针指向

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  // 量成员变量必须在初始化列表中显式初始化
  const int fd_;
  const std::string filename_;
};

// 跟踪被 PosixEnv::LockFile(). 锁住的文件
// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
class PosixLockTable {
 public:
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second;
    mu_.Unlock();
    return succeeded;
  }
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static const char msg[] =
        "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    *result = nullptr;
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!mmap_limiter_.Acquire()) {  // 如果有可用资源
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      void* mmap_base =
          ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {  //
        *result = new PosixMmapReadableFile(filename,
                                            reinterpret_cast<char*>(mmap_base),
                                            file_size, &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }
  /* O_TRUNC 如果文件已经存在,则将其长度截断为0  O_CLOEXEC
   * 避免文件描述符无意间泄露给fork创建的子进程
   * https://blog.csdn.net/l00102795/article/details/129978467
   * 一般子进程会调用exec执行另一个程序,此时会用全新的程序替换子进程的正文,数据,堆栈,此时保存文件
   * 描述符的变量也不存在了,导致无法关闭无用的文件描述符,所以fork子进程后在子进程中直接执行close
   * 关掉无用的文件描述符,然后再执行exec
   * */
  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    SPDLOG_LOGGER_INFO(SpdLogger::Log(), "create writeable file {}", filename);
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    return ::access(filename.c_str(), F_OK) == 0;  // F_OK 检查文件是否存在
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();
    ::DIR* dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }

  Status RemoveFile(const std::string& filename) override {
    if (::unlink(filename.c_str()) != 0) {  // 删除已存在文件
      return PosixError(filename, errno);
    }
    return Status::OK();
  }

  Status CreateDir(const std::string& dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {  // 目录 读写执行权限
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status RemoveDir(const std::string& dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) {
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags,
                    0644);  // 创建 LOCK 文件
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!locks_.Insert(filename)) {  // 要加锁的文件添加到locks_
      ::close(fd);
      return Status::IOError("lock " + filename, "already held by process");
    }

    if (LockOrUnlock(fd, true) == -1) {  // true 加锁 false 解锁
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename);
      return PosixError("lock " + filename, lock_errno);
    }

    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach();
  }

  Status GetTestDirectory(std::string* result) override {
    const char* env = std::getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The CreateDir status is ignored because the directory may already exist.
    CreateDir(*result);

    return Status::OK();
  }
  // 创建 LOG 文件
  Status NewLogger(const std::string& filename, Logger** result) override {
    // open 一个系统调用函数，用于打开一个文件，并返回文件描述符
    int fd =
        ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags,
               0644);  // 文件所有者有读写权限，其他用户只有读权限
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }
    // 将给定的文件描述符打开成一个文件流，并将该文件流的指针返回
    std::FILE* fp = ::fdopen(fd, "w");
    if (fp == nullptr) {
      ::close(fd);
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();
  // 后台主线程函数入口点
  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe because it is immutable.
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_
      GUARDED_BY(background_work_mutex_);  // 绑定互斥锁 background_work_mutex_
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);

  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;    // Thread-safe.
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() { return g_mmap_limit; }

// Return the maximum number of read-only files to keep open.
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
#ifdef __Fuchsia__
  // Fuchsia doesn't implement getrlimit.
  g_open_read_only_file_limit = 50;
#else
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
    // getrlimit failed, fallback to hard-coded default.
    g_open_read_only_file_limit = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of 20% of available file descriptors for read-only files.
    g_open_read_only_file_limit = rlim.rlim_cur / 5;
  }
#endif
  return g_open_read_only_file_limit;
}

}  // namespace
// https://stackoverflow.com/questions/75851752/about-leveldbs-implementation-of-posixenvschedule-i-have-a-question-why
PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),  // 初始化条件变量
      started_background_thread_(false),             // 默认不能创建消费者线程
      mmap_limiter_(MaxMmaps()),                     // 限制最大的mmap数量
      fd_limiter_(MaxOpenFiles()) {}  // 限制能打开的最大文件描述符
// 任务调度器，生产者，将要执行的<函数，传入参数>将任务添加到队列中
void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();
  // 如果没有开启后台线程，则创建后台线程，保证只有一个消费者线程
  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {  // 默认false
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint,
                                  this);  // 创建一个线程任务
    background_thread.detach();
  }
  // 如果已经开启线程，如果任务队列不为空，消费者线程不处于等待状态，进入这里说明锁已释放，消费者线程在执行任务
  // 发送信号写在这里是因为 BackgroundThreadMain
  // 函数要先获得锁再判断任务队列是否为空 If the queue is empty, the background
  // thread may be waiting for work.
  if (background_work_queue_
          .empty()) {              // 如果工作队列不为空，则消费者线程不会等待
    background_work_cv_.Signal();  // 发送信号时不关心是否有线程在等待
  }
  // 插入待处理任务，生产者
  background_work_queue_.emplace(background_work_function, background_work_arg);
  // 释放锁，消费者开始消费，如果消费者执行任务时间较长，该线程可以重新获得锁，继续添加任务
  background_work_mutex_.Unlock();
}
// 后台主线程，消费者,该线程是一个常驻线程，可以不停的向 background_work_queue_
// 提交任务，执行完成之后就 wait
void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_
        .Lock();  // 这里会等待上面Schedule 函数background_work_mutex_.Unlock()
                  // 释放锁，才执行下面代码
    // 这里的共享变量是 background_work_mutex_
    // Wait until there is work to be done. background_work_queue_
    // 保存的是函数和要执行的参数
    while (
        background_work_queue_
            .empty()) {  // 后台工作队列是空的，就一直等待,直到有任务需要处理，这里是一个消费者
      // wait 后线程进入休眠状态，被放入一个等待队列中，等待信号
      background_work_cv_
          .Wait();  // 等待生产者发送信号，此时其他线程可能在写数据
      // wait 后是先释放锁 background_work_mutex_，此时生产者 Schedule 持有该锁
      // Schedule(生产者) 中发送信号后，锁并没有被释放，消费者还需要 wait
      // Schedule 释放锁后，生产者消费者重新竞争锁
      // 如果消费者线程获得锁 ，即 background_work_cv_ 重新获得锁，跳出 wait
      //        若发现 background_work_queue_不为空，跳出 while 循环
      // 如果生产者线程获得锁，则继续将任务添加到任务队列中
    }

    assert(!background_work_queue_.empty());
    auto background_work_function =
        background_work_queue_.front().function;                     // 获取函数
    void* background_work_arg = background_work_queue_.front().arg;  // 获取参数
    background_work_queue_.pop();  // 弹出已取的元素

    background_work_mutex_
        .Unlock();  // 解锁后才开始执行任务，生产者队列继续添加任务
    background_work_function(background_work_arg);
  }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)  // NDEBUG 是standard C
                      // 中定义的宏,专门用过来控制assert()的行为,
    env_initialized_.store(true, std::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(
        sizeof(env_storage_) >= sizeof(EnvType),  // 静态断言，在编译时检查断言
        "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType),
                                alignof(EnvType)>::type  // aligof 返回对齐要求
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb
