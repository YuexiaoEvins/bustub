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
  if (header_page_id_ == INVALID_PAGE_ID){
    return false;
  }
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.template As<ExtendibleHTableHeaderPage>();
  uint32_t hash_key = Hash(key);
  uint32_t dir_id = header_page->HashToDirectoryIndex(hash_key);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_id);
  header_guard.Drop();
  if (dir_page_id == INVALID_PAGE_ID){
    return false;
  }

  ReadPageGuard dir_page_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_page_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  dir_page_guard.Drop();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K,V,KC>>();
  V value;
  bool lookup_success = bucket_page->Lookup(key,value, cmp_);
  if (lookup_success) {
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
  if (header_page_id_ == INVALID_PAGE_ID){
    return false;
  }

  auto header_write_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_write_guard.template AsMut<ExtendibleHTableHeaderPage>();
  uint32_t hash_key = Hash(key);

  uint32_t dir_id = header_page->HashToDirectoryIndex(hash_key);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_id);
  if (dir_page_id == INVALID_PAGE_ID){
    BasicPageGuard dir_guard = bpm_->NewPageGuarded(&dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    dir_page->Init(directory_max_depth_);
    header_page->SetDirectoryPageId(dir_id,dir_page_id);
  }
  header_write_guard.Drop();

  WritePageGuard dir_page_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_page_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    BasicPageGuard bucket_basic_guard = bpm_->NewPageGuarded(&bucket_page_id);
    auto bucket_write_guard = bucket_basic_guard.UpgradeWrite();
    auto bucket_page = bucket_write_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    bucket_page->Init(bucket_max_size_);
    dir_page->SetBucketPageId(bucket_index,bucket_page_id);
    dir_page->SetLocalDepth(bucket_index,0);
  }

  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  V temp_value;
  if (bucket_page->Lookup(key,temp_value,cmp_)){
    return false;
  }

  if (!bucket_page->IsFull()) {
    return bucket_page->Insert(key,value,cmp_);
  }

  //bucket is full


  if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_index)){
    if (dir_page->GetMaxDepth() <= dir_page->GetGlobalDepth()) {
      return false;
    }
    dir_page->IncrGlobalDepth();
  }

  dir_page->IncrLocalDepth(bucket_index);
  bool split_ret = SplitBucket(dir_page, bucket_page, bucket_index);
  if (!split_ret){
    return false;
  }

  //after split bucket
  dir_page_guard.Drop();
  bucket_page_guard.Drop();
  return Insert(key,value,transaction);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                              ExtendibleHTableBucketPage<K,V,KC> *bucket, uint32_t bucket_idx) -> bool {
  uint32_t split_bucket_id = directory->GetSplitImageIndex(bucket_idx);
  page_id_t split_page_id;
  auto split_page_basic_guard = bpm_->NewPageGuarded(&split_page_id);
  auto split_page_write_guard = split_page_basic_guard.UpgradeWrite();
  auto split_bucket = split_page_write_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  split_bucket->Init(bucket_max_size_);

  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  directory->SetBucketPageId(split_bucket_id,split_page_id);
  directory->SetLocalDepth(split_bucket_id,local_depth);

  uint32_t diff = 1 << local_depth;
  for (int i = bucket_idx + diff; i < static_cast<int>(directory->Size()); i+=diff) {
    directory->SetLocalDepth(i,local_depth);
  }
  for (int i = bucket_idx - diff; i >= 0; i-=diff) {
    directory->SetLocalDepth(i,local_depth);
  }
  for (int i = split_bucket_id + diff; i < static_cast<int>(directory->Size()); i+=diff) {
    directory->SetBucketPageId(i, split_page_id);
    directory->SetLocalDepth(i, local_depth);
  }

  for (int i = split_bucket_id - diff; i >= 0; i-=diff) {
    directory->SetBucketPageId(i,split_page_id);
    directory->SetLocalDepth(i,local_depth);
  }

  std::vector<std::pair<K, V>> entries;
  for (uint32_t i = 0; i < bucket->Size(); ++i) {
    entries.push_back(bucket->EntryAt(i));
  }
  bucket->Clear();

  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  for (auto &entry : entries) {
    uint32_t rehash_bucket_index = directory->HashToBucketIndex(Hash(entry.first));
    page_id_t rehash_bucket_page_id = directory->GetBucketPageId(rehash_bucket_index);
    assert(rehash_bucket_page_id == bucket_page_id || rehash_bucket_page_id == split_page_id);
    if (rehash_bucket_page_id == bucket_page_id) {
      bucket->Insert(entry.first,entry.second,cmp_);
    } else {
      split_bucket->Insert(entry.first,entry.second,cmp_);
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
