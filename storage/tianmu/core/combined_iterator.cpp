//
// Created by dfx on 22-12-19.
//

#include "combined_iterator.h"

namespace Tianmu::core {

CombinedIterator::CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs, const Filter &filter) {
  delta_iter_ = std::make_unique<DeltaIterator>(table->GetDelta().get(), attrs);
  base_iter_ = std::make_unique<TianmuIterator>(table, attrs, filter);
  is_delta_ = delta_iter_->Valid() ? true : false;  // first read delta, then read base
}

CombinedIterator::CombinedIterator(TianmuTable *table, const std::vector<bool> &attrs) {
  delta_iter_ = std::make_unique<DeltaIterator>(table->GetDelta().get(), attrs);
  base_iter_ = std::make_unique<TianmuIterator>(table, attrs);
  is_delta_ = delta_iter_->Valid() ? true : false;
}

bool CombinedIterator::operator==(const CombinedIterator &o) {
  return is_base_ == o.is_base_ && (is_base_ ? base_iter_ == o.base_iter_ : delta_iter_ == o.delta_iter_);
}

bool CombinedIterator::operator!=(const CombinedIterator &other) { return !(*this == other); }

void CombinedIterator::Next() {
  if (is_delta_) {
    delta_iter_->Next();
    if (!delta_iter_->Valid()) {
      is_delta_ = false;
void CombinedIterator::Next() {
  if (is_base_) {
    if (base_iter_->Valid()) {
      base_iter_->Next();
      if (!base_iter_->Valid()) {  // switch to delta
        delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
        is_base_ = false;
      }
    }
  } else {
    base_iter_->Next();
  }
}

std::shared_ptr<types::TianmuDataType> &CombinedIterator::GetBaseData(int col) { return base_iter_->GetData(col); }

std::string CombinedIterator::GetDeltaData() { return delta_iter_->GetData(); }

void CombinedIterator::SeekTo(int64_t row_id) {
  int64_t delta_start_pos = delta_iter_->StartPosition();
  if (row_id >= delta_start_pos) {  // delta
    delta_iter_->SeekTo(row_id);
    is_delta_ = true;
  } else {  // base
    base_iter_->SeekTo(row_id);
    is_delta_ = false;
void CombinedIterator::SeekTo(int64_t row_id) {
  int64_t base_max_row_id = base_table_->NumOfObj();
  if (row_id <= base_max_row_id) {
    base_iter_->SeekTo(row_id);
    is_base_ = true;
  } else {
    if (!delta_iter_) {  // lazy create
      delta_iter_ = std::make_unique<DeltaIterator>(base_table_->GetDelta().get(), attrs_);
    }
    delta_iter_->SeekTo(row_id);
    is_base_ = false;
  }
}

int64_t CombinedIterator::Position() const {
  if (is_base_) {
    return base_iter_->Position();
  } else {
    return delta_iter_->Position();
  }
}

bool CombinedIterator::Valid() const { return Position() != -1; }

bool CombinedIterator::IsBase() const { return is_base_; }

}  // namespace Tianmu::core
