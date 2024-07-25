//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    return false;
  }

  for (auto it = history_list_.rbegin();it != history_list_.rend();it++) {
    auto frame = *it;
    if (is_evictable_map_[frame]) {
      access_count_[frame] = 0;
      history_list_.erase(history_map_[frame]);
      history_map_.erase(frame);
      curr_size_--;
      is_evictable_map_[frame] = false;
      *frame_id = frame;
      return true;
    }
  }


  for (auto it = cache_list_.rbegin();it != cache_list_.rend();it++) {
    auto frame = *it;
    if (is_evictable_map_[frame]) {
      access_count_[frame] = 0;
      cache_list_.erase(cache_map_[frame]);
      cache_map_.erase(frame);
      curr_size_--;
      is_evictable_map_[frame] = false;
      *frame_id = frame;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<int>(replacer_size_), "Frame ID out of range.");
  access_count_[frame_id]++;
  if (access_count_[frame_id] < k_) {
    history_list_.push_front(frame_id);
    history_map_[frame_id] = history_list_.begin();
    return;
  }

  if (access_count_[frame_id] == k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
    return;
  }

  //frame_id over k times
  if (cache_map_.count(frame_id) > 0) {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
  }
  cache_list_.push_front(frame_id);
  cache_map_[frame_id] = cache_list_.begin();
  return;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<int>(replacer_size_), "Frame ID out of range.");

  if (access_count_[frame_id] == 0) {
    return;
  }

  if (true == is_evictable_map_[frame_id] && false == set_evictable) {
    curr_size_--;
  }

  if (false == is_evictable_map_[frame_id] && true == set_evictable) {
    curr_size_++;
  }

  is_evictable_map_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id < static_cast<int>(replacer_size_), "Frame ID out of range.");
  BUSTUB_ASSERT(!is_evictable_map_[frame_id], "Frame ID is non-evictable.");

  if (access_count_[frame_id] == 0){
    return;
  }

  if(access_count_[frame_id] >= k_) {
    cache_list_.erase(cache_map_[frame_id]);
    cache_map_.erase(frame_id);
  } else {
    history_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
  }

  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_map_[frame_id] = false;
  return;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
