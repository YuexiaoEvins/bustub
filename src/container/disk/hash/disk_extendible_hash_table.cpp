//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {

  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.As<ExtendibleHTableHeaderPage>();

  // fetch the directory page
  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  header_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory = directory_guard.As<ExtendibleHTableDirectoryPage>();

  // fetch the bucket page
  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  directory_guard.Drop();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // look up the key in the bucket
  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // fetch or create the directory page
  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    BasicPageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id);
    directory_guard.AsMut<ExtendibleHTableDirectoryPage>()->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
  }
  header_guard.Drop();
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // fetch or create the bucket page
  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    BasicPageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
    bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    directory->SetLocalDepth(bucket_idx, 0);
  }
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V v;
  if (bucket->Lookup(key, v, cmp_)) {
    return false;
  }

  // split the bucket if it is already full
  if (bucket->IsFull()) {
    // scale out the directory if needed
    if (directory->GetLocalDepth(bucket_idx) == directory->GetGlobalDepth()) {
      if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {
        return false;
      }
      directory->IncrGlobalDepth();
    }
    // get the local depth ready for splitting
    directory->IncrLocalDepth(bucket_idx);

    if (!SplitBucket(directory, bucket, bucket_idx)) {
      return false;
    }
    directory_guard.Drop();
    bucket_guard.Drop();
    return Insert(key, value, transaction);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                              ExtendibleHTableBucketPage<K,V,KC> *bucket, uint32_t bucket_idx) -> bool {
  // create the split bucket and insert it into the directory
  page_id_t split_page_id;
  BasicPageGuard split_bucket_guard = bpm_->NewPageGuarded(&split_page_id);
  if (split_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto split_bucket = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket->Init();

  uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  directory->SetBucketPageId(split_idx, split_page_id);
  directory->SetLocalDepth(split_idx, local_depth);

  // populate all split bucket pointers
  uint32_t idx_diff = 1 << local_depth;
  for (int i = bucket_idx - idx_diff; i >= 0; i -= idx_diff) {
    directory->SetLocalDepth(i, local_depth);
  }
  for (int i = bucket_idx + idx_diff; i < static_cast<int>(directory->Size()); i += idx_diff) {
    directory->SetLocalDepth(i, local_depth);
  }
  for (int i = split_idx - idx_diff; i >= 0; i -= idx_diff) {
    directory->SetBucketPageId(i, split_page_id);
    directory->SetLocalDepth(i, local_depth);
  }
  for (int i = split_idx + idx_diff; i < static_cast<int>(directory->Size()); i += idx_diff) {
    directory->SetBucketPageId(i, split_page_id);
    directory->SetLocalDepth(i, local_depth);
  }

  // redistribute key value pairs among newly split buckets
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  int size = bucket->Size();
  std::list<std::pair<K, V>> entries;
  for (int i = 0; i < size; i++) {
    entries.push_back(bucket->EntryAt(i));
  }
  bucket->Clear();

  for (const auto &entry : entries) {
    uint32_t target_idx = directory->HashToBucketIndex(Hash(entry.first));
    page_id_t target_page_id = directory->GetBucketPageId(target_idx);
    assert(target_page_id == bucket_page_id || target_page_id == split_page_id);
    if (target_page_id == bucket_page_id) {
      bucket->Insert(entry.first, entry.second, cmp_);
    } else if (target_page_id == split_page_id) {
      split_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
//  page_id_t new_dir_page_id;
//  auto new_dir_page_basic_guard = bpm_->NewPageGuarded(&new_dir_page_id);
//  auto new_dir_page_write_guard = new_dir_page_basic_guard.UpgradeWrite();
//  auto new_dir_page = new_dir_page_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
//
//  header->SetDirectoryPageId(directory_idx,new_dir_page_id);
//  new_dir_page->Init(directory_max_depth_);
//  uint32_t bucket_index = new_dir_page->HashToBucketIndex(hash);
//  return InsertToNewBucket(new_dir_page,bucket_index,key,value);
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
//  page_id_t new_bucket_page_id;
//  auto new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
//  auto new_bucket_page_write_guard = new_bucket_page_guard.UpgradeWrite();
//  auto new_bucket_page = new_bucket_page_write_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
//  assert(new_bucket_page_id != INVALID_PAGE_ID);
//
//  directory->SetBucketPageId(bucket_idx,new_bucket_page_id);
//  directory->SetLocalDepth(bucket_idx,0);
//
//  new_bucket_page->Init(bucket_max_size_);
//  return new_bucket_page->Insert(key,value,cmp_);

  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_write_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_write_guard.template AsMut<ExtendibleHTableHeaderPage>();
  uint32_t hash_key = Hash(key);
  uint32_t dir_id = header_page->HashToDirectoryIndex(hash_key);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_id);
  header_write_guard.Drop();
  if (dir_page_id == INVALID_PAGE_ID){
    return false;
  }

  auto dir_write_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_write_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_write_guard = bpm_->FetchPageWrite(bucket_page_id);
  ExtendibleHTableBucketPage<K,V,KC> *bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();

  bool remove_ret = bucket_page->Remove(key,cmp_);
  if (!remove_ret) {
    return false;
  }

  if (!bucket_page->IsEmpty()) {
    return true;
  }

  TryMergeBucketRecursive(dir_page,bucket_page,bucket_index);
  while(dir_page->CanShrink()){
    dir_page->DecrGlobalDepth();
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::TryMergeBucketRecursive(ExtendibleHTableDirectoryPage *directory,
                                                                ExtendibleHTableBucketPage<K, V, KC> *bucket,
                                                                uint32_t bucket_idx) const {
  while(true){
    if (directory->GetLocalDepth(bucket_idx) == 0){
      return;
    }
    //get split bucket
    uint32_t split_image_bucket_index  = directory->GetSplitImageIndex(bucket_idx);
    page_id_t split_image_bucket_page_index = directory->GetBucketPageId(split_image_bucket_index);
    if (split_image_bucket_page_index == INVALID_PAGE_ID) {
      return;
    }

    auto split_image_bucket_guard = bpm_->FetchPageWrite(split_image_bucket_page_index);
    ExtendibleHTableBucketPage<K,V,KC> *split_image_bucket = split_image_bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();

    if (!split_image_bucket->IsEmpty()){
      return;
    }

    if (directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(split_image_bucket_index)) {
      return;
    }

    split_image_bucket->Clear();
    split_image_bucket_guard.Drop();

    page_id_t bucket_page_index = directory->GetBucketPageId(bucket_idx);
    directory->DecrLocalDepth(bucket_idx);
    uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
    uint32_t diff = 1 << local_depth;

    for (int i = bucket_idx + diff; i < static_cast<int>(directory->Size()); i+=diff) {
      directory->SetBucketPageId(i,bucket_page_index);
      directory->SetLocalDepth(i,local_depth);
    }
    for (int i = bucket_idx - diff; i >= 0; i-=diff) {
      directory->SetBucketPageId(i,bucket_page_index);
      directory->SetLocalDepth(i,local_depth);
    }
  }
}


template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
