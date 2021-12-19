//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // 分配页作为 Directory Page
  page_id_t page_id;
  Page* page = this->buffer_pool_manager_->NewPage(&page_id);
  if(page != nullptr){
    this->directory_page_id_ = page_id;
    // 为 Directory Page 设置元数据
    auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    directory_page->SetLSN(1);
    directory_page->SetPageId(page_id);
    directory_page->global_depth_ = 1;
  }

  // 分配页作为 Bucket Page, 初始的深度为1, 因此只需要分配2页即可
  for(size_t i = 0; i < 2; i++){
    page = this->buffer_pool_manager_->NewPage(&page_id);
    if(page_id != nullptr) {
      directory_page->SetBucketPageId(i, page_id);
      directory_page->SetLocalDepth(i, 1);
    }
  }
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return nullptr;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return nullptr;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.RLock();
  // 获取 Directory Page
  auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
  auto global_depth_mask = directory_page->GetGlobalDepthMask();
  // 获取 key hash 后的值
  uint32_t hash = this->Hash(key);
  // 根据 hash 后的值获取 bucket_idx
  uint32_t bucket_idx = hash & global_depth_mask;
  // 根据 bucket_idx 获取 Bucket Page
  size_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_page = reinterpret_cast<HashTableBucketPage*>(this->buffer_pool_manager_->FetchPage(bucket_page_id));
  if(bucket_page->IsFull()) {
    // 当 Bucket 满了之后直接返回 false, 不进行分离
    this->table_latch_.RUnlock();
    return false;
  }else{
    bucket_page->Insert(key, value, this->comparator_);
  }
  this->table_latch_.RUnlock();
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    this->table_latch_.RLock();
  // 获取 Directory Page
  auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
  auto global_depth_mask = directory_page->GetGlobalDepthMask();
  // 获取 key hash 后的值
  uint32_t hash = this->Hash(key);
  // 根据 hash 后的值获取 bucket_idx
  uint32_t bucket_idx = hash & global_depth_mask;
  // 根据 bucket_idx 获取 Bucket Page
  size_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_page = reinterpret_cast<HashTableBucketPage*>(this->buffer_pool_manager_->FetchPage(bucket_page_id));
  if(bucket_page->IsFull()) {
    // TODO: 当 Bucket 满了之后对 Bucket 进行分离
  }else{
    bucket_page->Insert(key, value, this->comparator_);
  }
  this->table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
