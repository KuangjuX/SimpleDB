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
    // 分配页作为 Bucket Page, 初始的深度为0, 因此只需要分配1页即可
    page = this->buffer_pool_manager_->NewPage(&page_id);
    if(page != nullptr){
      directory_page->SetBucketPageId(0, page_id);
      directory_page->SetLocalDepth(0,0);
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
HashTableDirectoryPage* HASH_TABLE_TYPE::GetDirPage(){
  return this->FetchDirectoryPage();
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
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_page_id = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
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
  this->table_latch_.WLock();
  // 获取 Directory Page
  auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_page_id = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
  HashTableBucketPage<KeyType, ValueType, KeyComparator>* bucket_page = this->FetchBucketPage(bucket_page_id);
  if(bucket_page->IsFull()) {
    // 当 Bucket 满了之后调用 SplitInsert
    bool res = this->SplitInsert(transaction, key, value);
    this->table_latch_.WUnlock();
    return res;
  }
  bool res = bucket_page->Insert(key, value, this->comparator_);
  this->table_latch_.WUnlock();
  return res;

}

/**
 * Performs insertion with an optional bucket splitting.
 *
 * @param transaction a pointer to the current transaction
 * @param key the key to insert
 * @param value the value to insert
 * @return whether or not the insertion was successful
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 获取 Directory Page
  auto directory_page = reinterpret_cast<HashTableDirectoryPage*>(this->buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_page_id = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
  HashTableBucketPage<KeyType, ValueType, KeyComparator>* bucket_page = this->FetchBucketPage(bucket_page_id);
  // 获取 global_depth
  uint32_t global_depth = directory_page->GetGlobalDepth();
  // 获取 bucket_idx
  uint32_t bucket_idx = this->KeyToDirectoryIndex(key, directory_page);
  // 获取 local_depth 和 local_depth_mask
  uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
  size_t local_depth_mask = directory_page->GetLocalDepthMask(bucket_idx);

  uint32_t flag = bucket_idx & local_depth_mask;
  if(local_depth < global_depth) {
    // local_depth 比 global_depth 小
    // 新分配页以进行分离
    page_id_t new_bucket_page_id;
    Page* new_page = this->buffer_pool_manager_->NewPage(&new_bucket_page_id);
    new_page->GetData();

    // 获取可扩展哈希表所有可用的插槽范围
    size_t global_size = 1 << global_depth;
    // 遍历所有插槽，获取对应的 bucket_idx 重新映射页
    for(size_t slot_idx = 0; slot_idx < global_size; slot_idx++){
      if((slot_idx & local_depth_mask) == flag) {
        size_t new_local_depth_mask = local_depth_mask + (1 << local_depth);
        if(((slot_idx & new_local_depth_mask) >> local_depth) == 0) {
          directory_page->SetBucketPageId(slot_idx, bucket_page_id);
          directory_page->SetLocalDepth(slot_idx, local_depth + 1);
        }else if(((slot_idx & new_local_depth_mask) >> local_depth) == 1) {
          directory_page->SetBucketPageId(slot_idx, new_bucket_page_id);
          directory_page->SetLocalDepth(slot_idx, local_depth + 1);
        }
      }
    }
    // 遍历原有的 bucket 的内容进行分离
    size_t bucket_size = bucket_page->Size();
    for(size_t i = 0; i < bucket_size; i++) {
      auto item_key = bucket_page->KeyAt(0);
      auto item_value = bucket_page->ValueAt(0);
      // 移除并重新进行插入
      bucket_page->Remove(item_key, item_value, this->comparator_);
      uint32_t item_bucket_page_id = this->KeyToPageId(item_key, directory_page);
      HashTableBucketPage<KeyType, ValueType, KeyComparator>* item_bucket_page = this->FetchBucketPage(item_bucket_page_id);
      item_bucket_page->Insert(item_key, item_value, this->comparator_);
    }

    bucket_page_id = this->KeyToPageId(key, directory_page);
    bucket_page = this->FetchBucketPage(bucket_page_id);
    bucket_page->Insert(key, value, this->comparator_);

  }else{
    // 此时 GLOBAL DEPTH 和 LOCAL DEPTH 相同，应当同时增长 GLOBAL DEPTH 和 LOCAL DEPTH
      size_t old_global_size = 1 << directory_page->GetGlobalDepth();
      directory_page->IncrGlobalDepth();

      for(size_t slot_idx = 0; slot_idx < old_global_size; slot_idx++) {
        // 遍历所有存在的 bucket page 并将其映射到新建的插槽中
        page_id_t bucket_item_page_id = directory_page->GetBucketPageId(slot_idx);
        uint32_t item_local_depth = directory_page->GetLocalDepth(bucket_item_page_id);
        uint32_t diff_bits = directory_page->GetGlobalDepth() - item_local_depth;
        // 计算应该映射到扩增的 directory bucket page 的 page id 映射
        size_t nums = 1 << diff_bits;
        for(size_t num = 0; num < nums; num++){
          uint32_t map_bucket_idx = (num << item_local_depth) + slot_idx;
          directory_page->SetBucketPageId(map_bucket_idx, bucket_item_page_id);
          directory_page->SetLocalDepth(map_bucket_idx, item_local_depth);
        }
      }

      // 此时 GLOBAL DEPTH 已经扩增完毕，并将所有 bucket_idx 进行重新映射，此时进行递归
      this->SplitInsert(transaction, key, value);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.WLock();
  auto directory_page = this->FetchDirectoryPage();
  // 根据 key 和 directory 获取 bucket_idx
  uint32_t bucket_page_id = this->KeyToPageId(key, directory_page);
  // 根据 bucket_id 获取 bucket_page
  HashTableBucketPage<KeyType, ValueType, KeyComparator>* bucket_page = this->FetchBucketPage(bucket_page_id);
  if(bucket_page->Remove(key, value, this->comparator_)){
    if(bucket_page->IsEmpty()) {
      this->Merge(transaction, key, value);
    }
    this->table_latch_.WUnlock();
    return true;
  }
  this->table_latch_.WUnlock();
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
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 此时 key hash 出来的对应的某个 bucket 为空, 应当对其进行合并
  HashTableDirectoryPage* dir_page = this->FetchDirectoryPage();
  uint32_t bucket_idx = this->Hash(key) & dir_page->GetGlobalDepthMask();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
  if(local_depth == 0){
    // LOCAL DEPTH 为 0，直接返回
    return;
  }
  // 找出所有在 LOCAL DEPTH 减少后应当重新映射的 bucket_idx, 并将另一个 bucket_page_id 映射到
  // 已经为空的插槽上, 并删除已经为空的页
  uint32_t local_mask_bucket_id = bucket_idx & dir_page->GetLocalDepthMask(bucket_idx);
  // 这里是要找到和它对称的那个 bucket_id
  uint32_t other_local_bucket_id = 0;
  if(local_mask_bucket_id > ((uint32_t)(1 << local_depth))) {
    uint32_t mask = 0;
    for(size_t i = 0; i < local_depth - 1; i++) {
      mask += 1 << i;
    }
    other_local_bucket_id = local_mask_bucket_id & mask;
  }else{
    uint32_t mask = 0;
    for(size_t i = 0; i < local_depth - 1; i++) {
      mask += 1 << i;
    }
    other_local_bucket_id = (local_mask_bucket_id & mask) + (1 << (local_depth - 1));
  }
  page_id_t other_bucket_page_id = dir_page->GetBucketPageId(other_local_bucket_id);
  // 找到所有相关的插槽，并将其 bucket_page_id 进行替换
  for(size_t slot_idx = 0; slot_idx < dir_page->Size(); slot_idx++){
    page_id_t bucket_page_item_id = dir_page->GetBucketPageId(slot_idx);
    if(bucket_page_item_id == bucket_page_id){
      dir_page->SetBucketPageId(slot_idx, other_bucket_page_id);
      dir_page->DecrLocalDepth(slot_idx);
    }
  }
  // 将已经空了的页移除
  this->buffer_pool_manager_->DeletePage(bucket_page_id);
}

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
