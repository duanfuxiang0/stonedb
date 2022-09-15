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

#include "rc_decimal.h"

#include <cmath>

#include "common/assert.h"
#include "core/tools.h"
#include "system/txt_utils.h"
#include "types/rc_data_types.h"
#include "types/value_parser4txt.h"

#include "types/rc_num.h"

namespace Tianmu {
namespace types {

RCDecimal::RCDecimal(common::CT attrt) : value_(common::NULL_VALUE_128), scale_(0), precision_(0), attr_type_(attrt) {}

RCDecimal::RCDecimal(BString value, short scale, short precision, common::CT attrt)
  : value_(value.ToString()), scale_(scale), precision_(precision), attr_type_(attrt) {
  null = (value_ != common::NULL_VALUE_128) ? false : true;
}

RCDecimal::RCDecimal(common::tianmu_int128_t value, short scale, short precision, common::CT attrt)
  : value_(value), scale_(scale), precision_(precision), attr_type_(attrt) {
  null = (value_ != common::NULL_VALUE_128) ? false : true;
}

RCDecimal::RCDecimal(const RCDecimal &rcn)
    : ValueBasic<RCDecimal>(rcn), value_(rcn.value_), scale_(rcn.scale_), precision_(rcn.precision_), attr_type_(rcn.attr_type_) {
  null = rcn.null;
}

RCDecimal::~RCDecimal() {}

RCDecimal &RCDecimal::Assign(BString value, short scale, short precision, common::CT attrt) {
  common::tianmu_int128_t t(value.ToString());
  this->value_ = t;
  this->scale_ = scale;
  this->precision_ = precision;
  this->attr_type_ = attrt;

  if (scale <= -1) scale_ = 0;
  null = (value_ == common::NULL_VALUE_128 ? true : false);
  return *this;
}

RCDecimal &RCDecimal::Assign(int64_t value, short scale, short precision, common::CT attrt) {
  common::tianmu_int128_t t = value;
  this->value_ = t;
  this->scale_ = scale;
  this->precision_ = precision;
  this->attr_type_ = attrt;

  if (scale <= -1) scale_ = 0;
  null = (value_ == common::NULL_VALUE_128 ? true : false);
  return *this;
}

common::ErrorCode RCDecimal::Parse(const BString &rcs, RCDecimal &rcn, uint precision, ushort scale, common::CT at) {
  // parse decimal from text
  return ValueParserForText::ParseDecimal(rcs, rcn, precision, scale);
}

common::ErrorCode RCDecimal::ParseReal(const BString &rcbs, RCDecimal &rcdc, common::CT at) {
  return ValueParserForText::ParseRealDecimal(rcbs, rcdc, at);
}

RCDecimal &RCDecimal::operator=(const RCDecimal &rcn) {
  value_ = rcn.value_;
  scale_ = rcn.scale_;
  precision_ = rcn.precision_;
  null = rcn.null;
  attr_type_ = rcn.attr_type_;
  return *this;
}

RCDecimal &RCDecimal::operator=(const RCValueObject &rvo) {
  const RCDataType& rcdt = *(rvo.Get());
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    RCNum& rcn = (RCNum&)rcdt;        
    RCNum rcn1 = rcn.ToDecimal();
    this->Assign(rcn1.ValueInt(), rcn1.Scale(), RCNum::MAX_DEC_PRECISION, common::CT::NUM);
  } else if (rcdt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE) {
    *this = (RCDecimal&)rcdt;
  } else {
    RCDecimal rcn1;
    if (common::IsError(RCDecimal::Parse(rcdt.ToBString(), rcn1, precision_, scale_, this->attr_type_))) {
      *this = rcn1;
    } else {
      TIANMU_ERROR("Unsupported assign operation!");
      null = true;
    }
  }
  return *this;
}

RCDecimal &RCDecimal::operator=(const RCDataType &rcdt) {
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    RCNum& rcn = (RCNum&)rcdt;        
    RCNum rcn1 = rcn.ToDecimal();
    this->Assign(rcn1.ValueInt(), rcn1.Scale(), RCNum::MAX_DEC_PRECISION, common::CT::NUM);
  } else if (rcdt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE) {
    *this = (RCDecimal&)rcdt;
  } else {
    RCDecimal rcn1;
    if (common::IsError(RCDecimal::Parse(rcdt.ToBString(), rcn1, precision_, scale_, this->attr_type_))) {
      *this = rcn1;
    } else {
      TIANMU_ERROR("Unsupported assign operation!");
      null = true;
    }
  }
  return *this;
}

common::CT RCDecimal::Type() const { return attr_type_; }

bool RCDecimal::IsInt() const {
  if ((value_ % Uint128PowOfTen(scale_)) != 0) {
    return false;
  }
  return true;
}

RCDecimal RCDecimal::ToInt() const { 
  common::tianmu_int128_t t = GetIntPart(); 
  return RCDecimal(t, scale_, precision_, common::CT::NUM);
}

BString RCDecimal::ToReal() const {
  common::tianmu_int128_t v = GetIntPart();
  common::tianmu_int128_t f = GetFracPart();
  std::string real = v.convert_to<std::string>() + "." + f.convert_to<std::string>();
  return BString(real.c_str(), real.length(), true);
}

RCDecimal RCDecimal::ToDecimal(int scale) const {
  common::tianmu_int128_t tmpv = 0;
  short tmpp = 0;
  int sign = 1;

  tmpv = this->value_;
  tmpp = this->scale_;
  if (scale != -1) {
    if (tmpp > scale)
      tmpv /= (int64_t)Uint64PowOfTen(tmpp - scale);
    else
      tmpv *= (int64_t)Uint64PowOfTen(scale - tmpp);
    tmpp = scale;
  }
  return RCDecimal(tmpv * sign, 0, tmpp, common::CT::NUM);
}

static char *Text(int64_t value_, char buf[], int scale) {
  bool sign = true;
  if (value_ < 0) {
    sign = false;
    value_ *= -1;
  }
  longlong2str(value_, buf, 10);
  int l = (int)std::strlen(buf);
  std::memset(buf + l + 1, ' ', 21 - l);
  int pos = 21;
  int i = 0;
  for (i = l; i >= 0; i--) {
    if (scale != 0 && pos + scale == 20) {
      buf[pos--] = '.';
      i++;
    } else {
      buf[pos--] = buf[i];
      buf[i] = ' ';
    }
  }

  if (scale >= l) {
    buf[20 - scale] = '.';
    buf[20 - scale - 1] = '0';
    i = 20 - scale + 1;
    while (buf[i] == ' ') buf[i++] = '0';
  }
  pos = 0;
  while (buf[pos] == ' ') pos++;
  if (!sign) buf[--pos] = '-';
  return buf + pos;
}

BString RCDecimal::ToBString() const {
  if (!IsNull()) {
    std::string val;
    val = value_.convert_to<std::string>();
    return BString(val.c_str(), val.length(), true);
  }
  return BString();
}

bool RCDecimal::operator==(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) return (compare((RCDecimal &)rcdt) == 0);
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) == 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (rcdt == this->ToBString());
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

bool RCDecimal::operator!=(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) return (compare((RCDecimal &)rcdt) != 0);
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) != 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (rcdt != this->ToBString());
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

bool RCDecimal::operator<(const RCDataType &rcdt) const {
  if (IsNull() || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) return (compare((RCDecimal &)rcdt) < 0);
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) < 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (this->ToBString() < rcdt);
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

bool RCDecimal::operator>(const RCDataType &rcdt) const {
  if (IsNull() || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) return (compare((RCDecimal &)rcdt) > 0);
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) > 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (this->ToBString() > rcdt);
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

bool RCDecimal::operator<=(const RCDataType &rcdt) const {
  if (IsNull() || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE) return (compare((RCDecimal &)rcdt) <= 0);
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    RCDecimal rcdc;
    rcdc = (RCNum&)rcdt;
    return (compare(rcdc) <= 0);
  }
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) <= 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (this->ToBString() <= rcdt);
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

bool RCDecimal::operator>=(const RCDataType &rcdt) const {
  if (null || rcdt.IsNull()) return false;
  if (rcdt.GetValueType() == ValueTypeEnum::DECIMAL_TYPE) return (compare((RCDecimal &)rcdt) >= 0);
  if (rcdt.GetValueType() == ValueTypeEnum::NUMERIC_TYPE) {
    RCDecimal rcdc;
    rcdc = (RCNum&)rcdt;
    return (compare(rcdc) >= 0);
  }
  if (rcdt.GetValueType() == ValueTypeEnum::DATE_TIME_TYPE) return (compare((RCDateTime &)rcdt) >= 0);
  if (rcdt.GetValueType() == ValueTypeEnum::STRING_TYPE) return (this->ToBString() >= rcdt);
  TIANMU_ERROR("Bad cast inside RCDecimal");
  return false;
}

RCDecimal &RCDecimal::operator-=(const RCDecimal &rcdc) {
  DEBUG_ASSERT(!null);
  if (IsNull() || rcdc.IsNull()) return *this;
  if (scale_ < rcdc.scale_) {
    value_ = (value_ * Uint128PowOfTen(rcdc.scale_ - scale_)) - rcdc.value_;
    scale_ = rcdc.scale_;
  } else {
    value_ -= (rcdc.value_ * Uint128PowOfTen(scale_ - rcdc.scale_));
  }
  return *this;
}

RCDecimal &RCDecimal::operator+=(const RCDecimal &rcdc) {
  DEBUG_ASSERT(!null);
  if (IsNull() || rcdc.IsNull()) return *this;
  if (scale_ < rcdc.scale_) {
    value_ = ((value_ * Uint128PowOfTen(rcdc.scale_ - scale_)) + rcdc.value_);
    scale_ = rcdc.scale_;
  } else {
    value_ += (rcdc.value_ * Uint128PowOfTen(scale_ - rcdc.scale_));
  }
  return *this;
}

RCDecimal &RCDecimal::operator*=(const RCDecimal &rcdc) {
  DEBUG_ASSERT(!null);
  if (IsNull() || rcdc.IsNull()) return *this;
  value_ *= rcdc.value_;
  scale_ += rcdc.scale_;
  return *this;
}

RCDecimal &RCDecimal::operator/=(const RCDecimal &rcdc) {
  DEBUG_ASSERT(!null);
  if (IsNull() || rcdc.IsNull()) return *this;
  if (scale_ < rcdc.scale_) {
    value_ = (value_ * Uint128PowOfTen(rcdc.scale_ - scale_)) / rcdc.value_;
    scale_ = rcdc.scale_;
  } else {
    value_ /= (rcdc.value_ * Uint128PowOfTen(scale_ - rcdc.scale_));
  }
  return *this;
}

RCDecimal RCDecimal::operator-(const RCDecimal &rcn) const {
  RCDecimal res(*this);
  return res -= rcn;
}

RCDecimal RCDecimal::operator+(const RCDecimal &rcn) const {
  RCDecimal res(*this);
  return res += rcn;
}

RCDecimal RCDecimal::operator*(const RCDecimal &rcn) const {
  RCDecimal res(*this);
  return res *= rcn;
}

RCDecimal RCDecimal::operator/(const RCDecimal &rcn) const {
  RCDecimal res(*this);
  return res /= rcn;
}

uint RCDecimal::GetHashCode() const { return uint(GetIntPart() * 1040021); }

int RCDecimal::compare(const RCDecimal &rcdc) const {
  if (IsNull() || rcdc.IsNull()) return false;
  if (scale_ != rcdc.scale_) {
    if (value_ < 0 && rcdc.value_ >= 0) return -1;
    if (value_ >= 0 && rcdc.value_ < 0) return 1;
    if (scale_ < rcdc.scale_) {
      common::tianmu_int128_t power_of_ten = Uint128PowOfTen(rcdc.scale_ - scale_);
      common::tianmu_int128_t tmpv = rcdc.value_ / power_of_ten;
      if (value_ > tmpv) return 1;
      if (value_ < tmpv || rcdc.value_ % power_of_ten > 0) return -1;
      if (rcdc.value_ % power_of_ten < 0) return 1;
      return 0;
    } else {
      common::tianmu_int128_t power_of_ten = Uint128PowOfTen(scale_ - rcdc.scale_);
      common::tianmu_int128_t tmpv = value_ / power_of_ten;
      if (tmpv < rcdc.value_) return -1;
      if (tmpv > rcdc.value_ || value_ % power_of_ten > 0) return 1;
      if (value_ % power_of_ten < 0) return -1;
      return 0;
    }
  } else
    return (value_ > rcdc.value_ ? 1 : (value_ == rcdc.value_ ? 0 : -1));
}

int RCDecimal::compare(const RCDateTime &rcdt) const {
  int64_t tmp;
  rcdt.ToInt64(tmp);
  return int(GetIntPart() - tmp);
}

short RCDecimal::GetDecStrLen() const {
  if (IsNull()) return 0;
  short res = scale_;
  common::tianmu_int128_t tmpi = value_ / Uint128PowOfTen(scale_);
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

short RCDecimal::GetDecIntLen() const {
  if (IsNull()) return 0;
  short res = 0;
  common::tianmu_int128_t tmpi = 0;
  tmpi = GetIntPart();
  while (tmpi != 0) {
    tmpi /= 10;
    res++;
  }
  return res;
}

short RCDecimal::GetDecFractLen() const {
  if (IsNull()) return 0;
  return scale_;
}

void RCDecimal::Negate() {
  if (IsNull()) return;
  value_ *= -1;
}

}  // namespace types
}  // namespace Tianmu