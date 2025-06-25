 // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include "db/log_writer.h"

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"

#include "gtest/gtest.h"

namespace leveldb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {  // 这里不是遍历n次，而是大小小于n
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  std::snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class LogTest : public testing::Test {
 public:
  LogTest()
      : reading_(false),
        writer_(new Writer(&dest_)),
        reader_(new Reader(&source_, &report_, true /*checksum*/,
                           0 /*initial_offset*/)) {}

  ~LogTest() {
    delete writer_;
    delete reader_;
  }

  void ReopenForAppend() {
    delete writer_;
    writer_ = new Writer(&dest_, dest_.contents_.size());
  }

  void Write(const std::string& msg) {
    ASSERT_TRUE(!reading_) << "Write() after starting to read";
    writer_->AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const { return dest_.contents_.size(); }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      source_.contents_ = Slice(dest_.contents_);
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_.contents_[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_.contents_[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_.contents_.resize(dest_.contents_.size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
    // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
    // 偏移6，即 4 checksum 和 2 length ，1 + len 为 1字节 type + len 字节数据
    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset + 6], 1 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_.contents_[header_offset], crc);
  }

  void ForceError() { source_.force_error_ = true; }

  size_t DroppedBytes() const { return report_.dropped_bytes_; }

  std::string ReportMessage() const { return report_.message_; }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

  void StartReadingAt(uint64_t initial_offset) {
    delete reader_;
    reader_ = new Reader(&source_, &report_, true /*checksum*/, initial_offset);
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader = new Reader(&source_, &report_, true /*checksum*/,
                                       WrittenBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(!offset_reader->ReadRecord(&record, &scratch));
    delete offset_reader;
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader =
        new Reader(&source_, &report_, true /*checksum*/, initial_offset);

    // Read all records from expected_record_offset through the last one.
    ASSERT_LT(expected_record_offset, num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_;  //
         ++expected_record_offset) {
      Slice record;
      std::string scratch;
      ASSERT_TRUE(offset_reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(initial_offset_record_sizes_[expected_record_offset],
                record.size());
      ASSERT_EQ(initial_offset_last_record_offsets_[expected_record_offset],
                offset_reader->LastRecordOffset());
      ASSERT_EQ((char)('a' + expected_record_offset), record.data()[0]);
    }
    delete offset_reader;
  }

 private:
  class StringDest : public WritableFile {
   public:
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    Status Append(const Slice& slice) override {
      contents_.append(slice.data(), slice.size());  // 将数据写到 contents
      return Status::OK();
    }

    std::string contents_;
  };
  /* 内部私有类 */
  class StringSource : public SequentialFile {
   public:
    StringSource() : force_error_(false), returned_partial_(false) {}

    Status Read(size_t n, Slice* result, char* scratch) override {
      EXPECT_TRUE(!returned_partial_) << "must not Read() after eof/error";

      if (force_error_) { // 可以手动设置 error
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }

      if (contents_.size() < n) {  // contents 就是目前存的日志数据大小
        n = contents_.size();
        returned_partial_ = true;  // 如果数据容量大小小于 n,
      }
      *result =
          Slice(contents_.data(), n);  // 根据保存的数据创建 Slice,并保存到
                                       // result 中,这里是指针指向,没有发生拷贝
      contents_.remove_prefix(n);  // 指针偏移,上面已经读取n 字节到result ,如果
                                   // contents_ 长度为n,remove后contents_指向空
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipped past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }

    Slice contents_;
    bool force_error_;
    bool returned_partial_;
  };

  class ReportCollector : public Reader::Reporter {
   public:
    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }

    size_t dropped_bytes_;
    std::string message_;
  };

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

  StringDest dest_;
  StringSource source_;
  ReportCollector report_;
  bool reading_;
  Writer* writer_;
  Reader* reader_;
};

size_t LogTest::initial_offset_record_sizes_[] = {
    10000,  // Two sizable records in first block
    10000,
    2 * log::kBlockSize - 1000,  // Span three blocks
    1,
    13716,                          // Consume all but two bytes of block 3.
    log::kBlockSize - kHeaderSize,  // Consume the entirety of block 4.
};

uint64_t LogTest::initial_offset_last_record_offsets_[] = {
    0,
    kHeaderSize + 10000,
    2 * (kHeaderSize + 10000),
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize +
        kHeaderSize + 1,
    3 * log::kBlockSize,
};

// LogTest::initial_offset_last_record_offsets_ must be defined before this.
int LogTest::num_initial_offset_records_ =  // 测试要写入的数据条数
    sizeof(LogTest::initial_offset_last_record_offsets_) / sizeof(uint64_t);

TEST_F(LogTest, Empty) { ASSERT_EQ("EOF", Read()); }

TEST_F(LogTest, ReadWriteSelf) {
  Write("ab");
  Write("abc");
  Write("abcd");
  ASSERT_EQ("ab", Read());
  ASSERT_EQ("abc", Read());
  ASSERT_EQ("abcd", Read());
}

TEST_F(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_F(LogTest, SNPRINTF) {
  char buf[10];
  int n = std::snprintf(buf, sizeof(buf), "%s.", "hello");
  std::cout << "n= " << n << " buf: " << buf << std::endl;

  /* 返回的长度是格式化后的长度,长度可能会大于 buf 的大小, 超出部分会截断 */
  int m = std::snprintf(buf, sizeof(buf), "%s.", "hello world C++");
  std::cout << "m= " << m << " buf: " << buf << std::endl;

  char buf1[50];
  int r = std::snprintf(buf1, sizeof(buf1), "%d.", 100000);
  std::cout << "r= " << r << " buf: " << buf1 << std::endl;
}

TEST_F(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("");  // 一个 block 刚好还剩 kHeaderSize 字节,可以写入一个 ""
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  const int n = kBlockSize - 2 * kHeaderSize;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize, WrittenBytes());
  Write("bar");  // 刚好还剩余 KHeaderSize 个字节,那么会将数据分成2条写入 First
                 // 存空字节,Last 存数据
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ShortTrailer) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  // n + kHeaderSize = 写入长度
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());

  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, AlignedEof) {
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  Write(BigString("foo", n));
  ASSERT_EQ(kBlockSize - kHeaderSize + 4, WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, OpenForAppend) {
  Write("hello");
  ReopenForAppend();
  Write("world");
  ASSERT_EQ("hello", Read());
  ASSERT_EQ("world", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, RandomString) {
  const int N = 10;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    std::cout << "i " << i << " " << RandomSkewedString(i, &write_rnd) << std::endl;
  }
}

TEST_F(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST_F(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_F(LogTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_F(LogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error.
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, BadLength) {
  const int kPayloadSize = kBlockSize - kHeaderSize;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4].
  // 将存储第一个record 长度加1，这样从下一个 record 开始读取
  IncrementByte(4, 1);
  ASSERT_EQ("foo", Read());
  ASSERT_EQ(kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("bad record length"));
}

TEST_F(LogTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_F(LogTest, ChecksumMismatch) {
  Write("foo");
  IncrementByte(0, 10);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(10, DroppedBytes());
  ASSERT_EQ("OK", MatchError("checksum mismatch"));
}

TEST_F(LogTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, kMiddleType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_F(LogTest, UnexpectedLastType) {
  Write("foo");
  SetByte(6, kLastType);
  FixChecksum(0, 3);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

// 设计成 kFirstType + kFullType
TEST_F(LogTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

// KFullType + K
TEST_F(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, kFirstType);
  FixChecksum(0, 3);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_F(LogTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
}

TEST_F(LogTest, SkipIntoMultiRecord) {
  // Consider a fragmented record:
  //    first(R1), middle(R1), last(R1), first(R2)
  // If initial_offset points to a record after first(R1) but before first(R2)
  // incomplete fragment errors are not actual errors, and must be suppressed
  // until a new first or full record is encountered.
  Write(BigString("foo", 3 * kBlockSize));
  Write("correct");
  StartReadingAt(kBlockSize);

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0, DroppedBytes());
  ASSERT_EQ("EOF", Read());
}

TEST_F(LogTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  ASSERT_EQ("correct", Read());
  ASSERT_EQ("EOF", Read());
  const size_t dropped = DroppedBytes();
  ASSERT_LE(dropped, 2 * kBlockSize + 100);
  ASSERT_GE(dropped, 2 * kBlockSize);
}

TEST_F(LogTest, ReadStart) { CheckInitialOffsetRecord(0, 0); }

TEST_F(LogTest, ReadSecondOneOff) { CheckInitialOffsetRecord(1, 1); }

TEST_F(LogTest, ReadSecondTenThousand) { CheckInitialOffsetRecord(10000, 1); }

TEST_F(LogTest, ReadSecondStart) { CheckInitialOffsetRecord(10007, 1); }

TEST_F(LogTest, ReadThirdOneOff) { CheckInitialOffsetRecord(10008, 2); }

TEST_F(LogTest, ReadThirdStart) { CheckInitialOffsetRecord(20014, 2); }

TEST_F(LogTest, ReadFourthOneOff) { CheckInitialOffsetRecord(20015, 3); }

TEST_F(LogTest, ReadFourthFirstBlockTrailer) {
  CheckInitialOffsetRecord(log::kBlockSize - 4, 3);
}

TEST_F(LogTest, ReadFourthMiddleBlock) {
  CheckInitialOffsetRecord(log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthLastBlock) {
  CheckInitialOffsetRecord(2 * log::kBlockSize + 1, 3);
}

TEST_F(LogTest, ReadFourthStart) {
  CheckInitialOffsetRecord(
      2 * (kHeaderSize + 1000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
      3);
}

TEST_F(LogTest, ReadInitialOffsetIntoBlockPadding) {
  CheckInitialOffsetRecord(3 * log::kBlockSize - 3, 5);
}

TEST_F(LogTest, ReadEnd) { CheckOffsetPastEndReturnsNoRecords(0); }

TEST_F(LogTest, ReadPastEnd) { CheckOffsetPastEndReturnsNoRecords(5); }

}  // namespace log
}  // namespace leveldb
