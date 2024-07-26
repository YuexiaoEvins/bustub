//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint32_t i = 0; i < MaxSize(); ++i) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (max_depth_ == 0) {
    return 0;
  }
  return hash & ((1 << global_depth_ ) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  assert(bucket_idx <= (pow(2, max_depth_)));
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx <= (pow(2, max_depth_)));
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth_mask = GetLocalDepthMask(bucket_idx);
  auto local_depth = GetLocalDepth(bucket_idx);
  // case:(LD==0)
  return (bucket_idx & local_depth_mask) ^ (1 << (local_depth - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  assert(global_depth_ <= max_depth_);
  return (1 << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t{
  auto local_depth = GetLocalDepth(bucket_idx);
  return (1 << local_depth) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ <= max_depth_);
  if (global_depth_ == max_depth_) {
    return;
  }

  auto old_size = Size();
  global_depth_++;
  auto new_size = Size();
  for (uint32_t i = old_size; i < new_size; ++i) {
    bucket_page_ids_[i] = bucket_page_ids_[i - old_size];
    local_depths_[i] = local_depths_[i - old_size];
  }
}


void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ > 0){
    global_depth_--;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ <= 0){
    return false;
  }
  for (uint32_t i = 0; i < Size(); ++i) {
    if (global_depth_ == local_depths_[i]){
      return false;
    }
  }

  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return (1 << global_depth_);
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  return (1 << max_depth_);
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
//  assert(bucket_idx <= (pow(2, max_depth_)));
  assert(bucket_idx >= 0);
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
//  assert(local_depth <= global_depth_);
//  assert(bucket_idx <= (pow(2, max_depth_)));
  assert(bucket_idx >= 0);

  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx <= (pow(2, max_depth_)));
  assert(bucket_idx >= 0);
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
