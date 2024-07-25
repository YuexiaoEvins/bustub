//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t replacement_frame_id;

  if (free_list_.empty()) {
    //try to get frame from replacer
    if(!replacer_->Evict(&replacement_frame_id)){
      return nullptr;
    }

    auto &previous_page = pages_[replacement_frame_id];
    if (previous_page.IsDirty()){
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule(DiskRequest{
          true,previous_page.GetData(),previous_page.GetPageId(),std::move(promise)});
      future.get();
    }
    page_table_.erase(previous_page.GetPageId());

  } else {
    replacement_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;

  pages_[replacement_frame_id].ResetMemory();
  pages_[replacement_frame_id].pin_count_ = 0;
  pages_[replacement_frame_id].is_dirty_ = false;
  pages_[replacement_frame_id].page_id_ = new_page_id;

  page_table_.insert(std::make_pair(new_page_id,replacement_frame_id));
  replacer_->SetEvictable(replacement_frame_id, false);
  replacer_->RecordAccess(replacement_frame_id);
  pages_[replacement_frame_id].pin_count_++;
  return &pages_[replacement_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    return &pages_[it->second];
  }

  //fetch from replacers
  frame_id_t replacement_frame_id;
  if (!free_list_.empty()){
    replacement_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&replacement_frame_id)) {
      return nullptr;
    };

    auto &previous_page = pages_[replacement_frame_id];
        if (previous_page.IsDirty()){
          auto promise = disk_scheduler_->CreatePromise();
          auto future = promise.get_future();
          disk_scheduler_->Schedule(DiskRequest{
                  true,previous_page.GetData(),previous_page.GetPageId(),std::move(promise)});
          future.get();
        }

        page_table_.erase(previous_page.GetPageId());
  }

  pages_[replacement_frame_id].ResetMemory();
  pages_[replacement_frame_id].pin_count_ = 0;
  pages_[replacement_frame_id].is_dirty_ = false;
  pages_[replacement_frame_id].page_id_ = page_id;

  auto read_promise = disk_scheduler_->CreatePromise();
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule(DiskRequest{
      false, pages_[replacement_frame_id].GetData(), page_id,std::move(read_promise)
  });

  page_table_.insert(std::make_pair(page_id,replacement_frame_id));
  replacer_->SetEvictable(replacement_frame_id, false);
  replacer_->RecordAccess(replacement_frame_id);
  pages_[replacement_frame_id].pin_count_++;
  read_future.get();

  return &pages_[replacement_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  auto &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }

  page.pin_count_--;
  if (page.pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id,true);
  }

  if (is_dirty) {
    page.is_dirty_ = is_dirty;
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()){
    return false;
  }

  auto &page = pages_[it->second];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule(DiskRequest{
      true, page.GetData(),page.GetPageId(),std::move(promise),
  });
  future.get();
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto &pair :page_table_) {
    page_id_t page_id = pair.first;
    BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");

    auto &page = pages_[pair.second];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(DiskRequest{
        true, page.GetData(),page.GetPageId(),std::move(promise),
    });
    future.get();
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "page_id is invalid");
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  frame_id_t target_frame_id = it->second;
  if (pages_[target_frame_id].pin_count_ > 0){
    return false;
  }

  page_table_.erase(page_id);
  free_list_.push_back(target_frame_id);
  pages_[target_frame_id].ResetMemory();
  pages_[target_frame_id].pin_count_ = 0;
  pages_[target_frame_id].is_dirty_ = false;
  pages_[target_frame_id].page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);
  return true;

}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {
    this, page,
  };
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  assert(page != nullptr);
  return {
      this, page,
  };
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  assert(page != nullptr);
  return {
      this, page,
  };
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto new_page = NewPage(page_id);
  assert(new_page != nullptr);
  return {
    this,
    new_page,
  };
}

}  // namespace bustub
