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
    directory_page->IncrGlobalDepth();
    // 分配页作为 Bucket Page, 初始的深度为1, 因此只需要分配2页即可
    for(size_t i = 0; i < 2; i++){
      page = this->buffer_pool_manager_->NewPage(&page_id);
      if(page != nullptr) {
        directory_page->SetBucketPageId(i, page_id);
        directory_page->SetLocalDepth(i, 1);
      }
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
void HASH_TABLE_TYPE::Spilt(size_t bucket_idx) {
  // auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
}

/**
 * KeyToDirectoryIndex - maps a key to a directory index
 *
 * In Extendible Hashing we map a key to a directory index
 * using the following hash + mask function.
 *
 * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
 *
 * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
 * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
 * representation.
 *
 * @param key the key to use for lookup
 * @param dir_page to use for lookup of global depth
 * @return the directory index
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t global_depth_mask = dir_page->GetGlobalDepthMask();
  uint32_t bucket_idx = this->Hash(key) & global_depth_mask;
  return bucket_idx;
}


/**
 * Get the bucket page_id corresponding to a key.
 *
 * @param key the key for lookup
 * @param dir_page a pointer to the hash table's directory page
 * @return the bucket page_id corresponding to the input key
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  // 获取 bucket_idx
  uint32_t bucket_idx = this->KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

/**
 * Fetches the directory page from the buffer pool manager.
 *
 * @return a pointer to the directory page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
  return directory_page;
}

/**
 * Fetches the a bucket page from the buffer pool manager using the bucket's page_id.
 *
 * @param bucket_page_id the page_id to fetch
 * @return a pointer to a bucket page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator>*>(this->buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
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
  // 根据 bucket_page_id 获取 bucket_page
  auto bucket_page = this->FetchBucketPage(bucket_page_id);
  // 从 Bucket Page 中获取 value
  if(bucket_page->GetValue(key, this->comparator_, result)) {
    this->table_latch_.RUnlock();
    return true;
  }
  this->table_latch_.RUnlock();
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
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_idx = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
  auto bucket_page = this->FetchBucketPage(bucket_idx);
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
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_idx = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
  auto bucket_page = this->FetchBucketPage(bucket_idx);
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
/**
 * Optionally merges an empty bucket into it's pair.  This is called by Remove,
 * if Remove makes a bucket empty.
 *
 * There are three conditions under which we skip the merge:
 * 1. The bucket is no longer empty.
 * 2. The bucket has local depth 0.
 * 3. The bucket's local depth doesn't match its split image's local depth.
 *
 * @param transaction a pointer to the current transaction
 * @param key the key that was removed
 * @param value the value that was removed
 */
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
