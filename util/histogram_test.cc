// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/histogram.h"

#include <limits>
#include <string>

#include "gtest/gtest.h"

namespace leveldb{

TEST(Histogram,H){
  Histogram histogram;

  histogram.Add(1l);
  histogram.Add(2l);
  histogram.Add(3l);
  histogram.Add(4l);
  histogram.Add(5l);

  std::cout <<  histogram.ToString() << std::endl;


}



}

