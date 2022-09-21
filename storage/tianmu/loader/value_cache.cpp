/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/

#include "loader/value_cache.h"

#include <algorithm>

#include "common/assert.h"
#include "types/rc_data_types.h"

namespace Tianmu {
namespace loader {

ValueCache::ValueCache(size_t valueCount, size_t initialCapacity) : value_count_(valueCount) {
  Realloc(initialCapacity);
  values_.reserve(valueCount);
  nulls_.reserve(valueCount);
}

void *ValueCache::Prepare(size_t valueSize) {
  size_t newSize(size_ + valueSize);
  if (newSize > capacity_) {
    auto newCapacity = capacity_;
    while (newSize > newCapacity) newCapacity <<= 1;

    auto vals = values_.size();
    if ((capacity_ > 0) && (vals > 0)) { /* second allocation, first reallocation */
      auto valSize = size_ / vals;
      newCapacity = std::max(newCapacity, valSize * value_count_);
    }
    Realloc(newCapacity);
  }

  if (!data_) return nullptr;
  return (static_cast<char *>(data_) + size_);
}

void ValueCache::CalcIntStats(std::optional<common::double_int_t> nv) {
  min_i_ = common::PLUS_INF_64, max_i_ = common::MINUS_INF_64, sum_i_ = 0;

  for (size_t i = 0; i < values_.size(); ++i) {
    if (!nulls_[i]) {
      auto v = *(int64_t *)GetDataBytesPointer(i);
      sum_i_ += v;

      if (min_i_ > v) min_i_ = v;
      if (max_i_ < v) max_i_ = v;
    }
  }

  if (NumOfNulls() > 0 && nv.has_value()) {
    if (min_i_ > nv->i) min_i_ = nv->i;
    if (max_i_ < nv->i) max_i_ = nv->i;
  }
}

void ValueCache::CalcRealStats(std::optional<common::double_int_t> nv) {
  min_d_ = common::PLUS_INF_64, max_d_ = common::MINUS_INF_64, sum_d_ = 0;
  for (size_t i = 0; i < values_.size(); ++i) {
    if (!nulls_[i]) {
      auto v = *(double *)GetDataBytesPointer(i);
      sum_d_ += v;

      if (min_d_ > v) min_d_ = v;
      if (max_d_ < v) max_d_ = v;
    }
  }

  if (NumOfNulls() > 0 && nv.has_value()) {
    if (min_d_ > nv->d) min_d_ = nv->d;
    if (max_d_ < nv->d) max_d_ = nv->d;
  }
}

void ValueCache::CalcStrStats(types::BString &min_s, types::BString &max_s, uint &maxlen,
                              const DTCollation &col) const {
  maxlen = 0;
  for (size_t i = 0; i < values_.size(); ++i) {
    if (!nulls_[i]) {
      types::BString v((Size(i) ? GetDataBytesPointer(i) : ZERO_LENGTH_STRING), Size(i));
      if (v.len > maxlen) maxlen = v.len;

      if (min_s.IsNull())
        min_s = v;
      else if (types::RequiresUTFConversions(col)) {
        if (CollationStrCmp(col, min_s, v) > 0) min_s = v;
      } else if (min_s > v)
        min_s = v;

      if (max_s.IsNull())
        max_s = v;
      else if (types::RequiresUTFConversions(col)) {
        if (CollationStrCmp(col, max_s, v) < 0) max_s = v;
      } else if (max_s < v)
        max_s = v;
    }
  }
  types::BString nv(ZERO_LENGTH_STRING, 0);
  if (NumOfNulls() > 0) {
    if (min_s > nv) min_s = nv;
    if (max_s < nv) max_s = nv;
  }
}

void ValueCache::CalcDecStats(
              types::BString &min_s, 
              types::BString &max_s, 
              types::BString &sum_s, 
              uint &maxlen, 
              const DTCollation &col) const {
  maxlen = 0;
  common::tianmu_int128_t sum_v_128 = 0;

  for (size_t i = 0; i < values_.size(); ++i) {
    if (!nulls_[i] && Size(i)) {
      types::BString v(GetDataBytesPointer(i), Size(i));
      if (v.size() > maxlen) maxlen = v.len;
      common::tianmu_int128_t v_128(std::string(v.begin(), v.size()));
      sum_v_128 += v_128;

      if (min_s.IsNull())
        min_s = v;
      else {
        common::tianmu_int128_t v_min_128(std::string(min_s.begin(), min_s.size()));
        if (v_128 < v_min_128) min_s = v;
      }

      if (max_s.IsNull())
        max_s = v;
      else {
        common::tianmu_int128_t v_max_128(std::string(max_s.begin(), max_s.size()));
        if (v_128 > v_max_128) max_s = v;
      }
    }
  }
  std::string ss = sum_v_128.convert_to<std::string>();
  sum_s = types::BString(ss.c_str(), ss.length(), true);
}

}  // namespace loader
}  // namespace Tianmu
