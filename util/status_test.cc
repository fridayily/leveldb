// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"

#include <iostream>
#include <utility>

#include "leveldb/slice.h"

#include "gtest/gtest.h"

namespace leveldb {

TEST(Status, MoveConstructor) {
  {
    Status ok = Status::OK();
    Status ok2 = std::move(ok);

    ASSERT_TRUE(ok2.ok());
  }

  {
    Status status = Status::NotFound("custom NotFound status message");
    Status status2 = std::move(status);

    ASSERT_TRUE(status2.IsNotFound());
    ASSERT_EQ("NotFound: custom NotFound status message", status2.ToString());
  }

  {
    Status self_moved = Status::IOError("custom IOError status message");
    std::cout << "---------" << std::endl;
    // Needed to bypass compiler warning about explicit move-assignment.
    Status& self_moved_reference = self_moved;
    self_moved_reference = std::move(self_moved);
  }


}


TEST(Status,MyTest){
  {
    Status s1 = Status::Corruption("Corruption happen","2023");
    Status s2 = s1;
    std::cout << s2.ToString() << std::endl;
  }


}

}  // namespace leveldb
