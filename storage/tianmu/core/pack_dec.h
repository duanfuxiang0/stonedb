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
#ifndef TIANMU_CORE_PACK_DEC_H_
#define TIANMU_CORE_PACK_DEC_H_
#pragma once

#include <unordered_set>

#include "core/pack.h"
#include "marisa.h"

namespace Tianmu {

namespace loader {
class ValueCache;
}

namespace core {

class PackDec final : public Pack {
 public:
  PackDec(DPN *dpn, PackCoordinate pc, ColumnShare *s);
  ~PackDec() {
    DestructionLock();
    Destroy();
  }

  // overrides
  std::unique_ptr<Pack> Clone(const PackCoordinate &pc) const override;
  void LoadDataFromFile(system::Stream *fcurfile) override;
  void UpdateValue(size_t i, const Value &v) override;
  void Save() override;

  types::BString GetValueBinary(int i) const override;

  void LoadValues(const loader::ValueCache *vc);
  bool IsTrie() const { return state_ == PackStrtate::PACK_TRIE; }
  bool Lookup(const types::BString &pattern, uint16_t &id);
  bool LikePrefix(const types::BString &pattern, std::size_t prefixlen, std::unordered_set<uint16_t> &ids);
  bool IsNotMatched(int row, uint16_t &id);
  bool IsNotMatched(int row, const std::unordered_set<uint16_t> &ids);

  static void ConvertBStringToBinChar(types::BString &str, std::vector<char> &res, bool &sign);
  static void ConvertBinCharToBString(std::vector<uint8_t> &res, bool sign, types::BString &str);

  static common::tianmu_int128_t ConvertVecToInt128(const char* arr);
  static void WriteInt128ToVec(common::tianmu_int128_t v, char* arr);

  void CopyToDPN(std::vector<char> &src, bool sign, char* dest, int len);

 protected:
  std::pair<UniquePtr, size_t> Compress() override;
  void CompressTrie();
  void Destroy() override;

 private:
  PackDec() = delete;
  PackDec(const PackDec&, const PackCoordinate &pc);

  void Prepare(int no_nulls);
  void AppendValue(const char *value, uint size) {
    if (size == 0) {
      SetPtrSize(dpn->nr, nullptr, 0);
    } else {
      SetPtrSize(dpn->nr, Put(value, size), size);
      data.sum_len += size;
    }
    dpn->nr++;
  }

  // Covert readable dec to binary char
  static void ConvertVecToBinChar(const char* arr, types::BString& str);

  size_t CalculateMaxLen() const;
  types::BString GetStringValueTrie(int ono) const;
  size_t GetSize(int ono) {
      return data.lens8[ono];
  }

  void SetSize(int ono, uint size) {
      data.lens8[ono] = (uint8_t)size;
  }

  // Make sure this is larger than the max length of CHAR/TEXT field of mysql.
  static const size_t DEFAULT_BUF_SIZE = 64_KB;

  struct buf {
    char *ptr;
    const size_t len;
    size_t pos;

    size_t capacity() const { return len - pos; }
    void *put(const void *src, size_t length) {
      ASSERT(length <= capacity());
      auto ret = std::memcpy(ptr + pos, src, length);
      pos += length;
      return ret;
    }
  };

  char *GetPtr(int i) const { return data.index[i]; }
  void SetPtr(int i, void *addr) { data.index[i] = reinterpret_cast<char *>(addr); }
  void SetPtrSize(int i, void *addr, uint size) {
    SetPtr(i, addr);
    SetSize(i, size);
  }

  enum class PackStrtate { PACK_ARRAY, PACK_TRIE };
  PackStrtate state_ = PackStrtate::PACK_ARRAY;
  marisa::Trie marisa_trie_;
  UniquePtr compressed_data_;
  uint16_t *ids_array_;
  struct {
    std::vector<buf> v;
    size_t sum_len;
    char **index;
    union {
      void *lens;
      uint8_t *lens8;
    };
    uint8_t len_mode;
  } data{};

  // high bit is sign
  static const int MAX_DPN_S = 16;

  void *Put(const void *src, size_t length) {
    if (data.v.empty() || length > data.v.back().capacity()) {
      auto sz = length * 2;
      data.v.push_back({(char *)alloc(sz, mm::BLOCK_TYPE::BLOCK_UNCOMPRESSED), sz, 0});
    }
    return data.v.back().put(src, length);
  }

  void SaveUncompressed(system::Stream *fcurfile);
  void LoadUncompressed(system::Stream *fcurfile);
  void LoadCompressed(system::Stream *fcurfile);
  void LoadCompressedTrie(system::Stream *fcurfile);
  void TransformIntoArray();

  int GetCompressBufferSize(size_t size);
};
}  // namespace core
}  // namespace Tianmu

#endif  // TIANMU_CORE_PACK_STR_H_
