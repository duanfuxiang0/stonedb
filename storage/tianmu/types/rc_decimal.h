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
#ifndef TIANMU_TYPES_RC_DECIMAL_H_
#define TIANMU_TYPES_RC_DECIMAL_H_
#pragma once

#include "types/rc_data_types.h"

namespace Tianmu {
namespace types {

class BString;

class RCDecimal : public ValueBasic<RCDecimal> {
  friend class ValueParserForText;
  friend class Engine;

 public:
  RCDecimal(common::CT attrt = common::CT::NUM);
  RCDecimal(BString value, short scale = -1, short prec = -1, common::CT attrt = common::CT::UNK);
  RCDecimal(common::tianmu_int128_t value, short scale = -1, short prec = -1, common::CT attrt = common::CT::UNK);
  RCDecimal(const RCDecimal &);
  RCDecimal(const RCDataType &rcn);

  ~RCDecimal();

  RCDecimal &Assign(BString value, short scale = -1, short prec = -1, common::CT attrt = common::CT::UNK);
  RCDecimal &Assign(int64_t value, short scale = -1, short prec = -1, common::CT attrt = common::CT::UNK);

  static common::ErrorCode Parse(const BString &rcs, RCDecimal &rcn, 
                      uint precision, ushort scale, common::CT at = common::CT::UNK);

  static common::ErrorCode ParseReal(const BString &, RCDecimal &rcdc, common::CT at = common::CT::UNK);

  RCDecimal &operator=(const RCDecimal &rcn);
  RCDecimal &operator=(const RCValueObject &rcdt);
  RCDecimal &operator=(const RCDataType &rcdt) override;

  common::CT Type() const override;

  bool operator==(const RCDataType &rcdt) const override;
  bool operator<(const RCDataType &rcdt) const override;
  bool operator>(const RCDataType &rcdt) const override;
  bool operator>=(const RCDataType &rcdt) const override;
  bool operator<=(const RCDataType &rcdt) const override;
  bool operator!=(const RCDataType &rcdt) const override;

  RCDecimal &operator-=(const RCDecimal &rcn);
  RCDecimal &operator+=(const RCDecimal &rcn);
  RCDecimal &operator*=(const RCDecimal &rcn);
  RCDecimal &operator/=(const RCDecimal &rcn);

  RCDecimal operator-(const RCDecimal &rcn) const;
  RCDecimal operator+(const RCDecimal &rcn) const;
  RCDecimal operator*(const RCDecimal &rcn) const;
  RCDecimal operator/(const RCDecimal &rcn) const;

  bool IsReal() const { return false; }
  bool IsInt() const;
  RCDecimal ToInt() const;

  BString ToBString() const override;
  RCDecimal ToDecimal(int scale = -1) const;
  BString ToReal() const;

  operator common::tianmu_int128_t() const { return GetIntPart(); }
  operator double() const;
  operator float() const { return (float)(double)*this; }

  short Scale() const { return scale_; }
  short Precision() const { return precision_; } 
  common::tianmu_int128_t ValueInt() const { return value_; }
  char *GetDataBytesPointer() const override { return (char *)&value_; }
  common::tianmu_int128_t GetIntPart() const { return value_ / Uint128PowOfTen(scale_); }
  common::tianmu_int128_t GetFracPart() const { return value_ % Uint128PowOfTen(scale_); }

  common::tianmu_int128_t GetValueInt() const { return value_; }

  short GetDecStrLen() const;
  short GetDecIntLen() const;
  short GetDecFractLen() const;

  uint GetHashCode() const override;
  void Negate();

  static constexpr int MAX_DEC_PRECISION = 32;

 private:
  int compare(const RCDecimal& rcn) const;
  int compare(const RCDateTime &rcn) const;

 private:
  common::tianmu_int128_t value_;
  ushort scale_;  // means 'scale' actually
  ushort precision_; // means 'precision' actually
  common::CT attr_type_;

 public:
  const static ValueTypeEnum value_type = ValueTypeEnum::DECIMAL_TYPE;
};

}  // namespace types
}  // namespace Tianmu

#endif  // TIANMU_TYPES_RC_DECIMAL_H_