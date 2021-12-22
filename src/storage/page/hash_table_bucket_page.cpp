//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_BUCKET_TYPE::Size() {
  size_t count = 0;
  for(size_t i = 0; i < BUCKET_SIZE; i++) {
    if(!this->IsOccupied(i)){
      break;
    }
    count++;
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  size_t size = this->Size();
  uint32_t count = 0;
  for(size_t bucket_idx = 0; bucket_idx < size; bucket_idx++){
    if(this->IsOccupied(bucket_idx) && this->IsReadable(bucket_idx)){
      if(cmp(this->array_[bucket_idx].first, key) == 0){
        count++;
        result->push_back(this->array_[bucket_idx].second);
      }
    }
  }
  return count > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t count = 0;
  for(size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if(!this->IsOccupied(bucket_idx)) {
      break;
    }else if (cmp(this->array_[bucket_idx].first, key) == 0 && this->array_[bucket_idx].second == value && this->IsReadable(bucket_idx)){
      return false;
    }
    count++;
  }
  this->array_[count] = MappingType(key, value);
  this->SetOccupied(count);
  this->SetReadable(count);
  // Debug
//  this->PrintBucket();

  return true;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  size_t count = 0;
  size_t size = this->Size();
  for(size_t bucket_idx = 0; bucket_idx < size; bucket_idx++){
    if(!this->IsOccupied(bucket_idx)) {
      break;
    }else if(cmp(this->array_[bucket_idx].first, key) == 0 && this->array_[bucket_idx].second == value && this->IsReadable(bucket_idx)) {
      count++;
//      this->SetNonReadable(bucket_idx);
//      this->SetNonOccupied(bucket_idx);
      // 不考虑效率，将后面的元素进行前移
      for(size_t x = bucket_idx; x < size - 1; x++) {
        this->array_[x] = this->array_[x+1];
        this->SetOccupied(x);
        this->SetReadable(x);
        this->SetNonOccupied(x+1);
        this->SetNonReadable(x+1);
      }
    }
  }
  // Debug
//  this->PrintBucket();
  return count > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return this->array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return this->array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
//  this->SetNonReadable(bucket_idx);
//    KeyType key = this->array_[bucket_idx].first;
  size_t size = this->Size();
  for(size_t x = bucket_idx; x < size - 1; x++) {
    this->array_[x] = this->array_[x+1];
    this->SetOccupied(x);
    this->SetReadable(x);
    this->SetNonOccupied(x+1);
    this->SetNonReadable(x+1);
  }

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = 1 << (bucket_idx % 8);
  return this->occupied_[slot_id] & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = 1 << (bucket_idx % 8);
  this->occupied_[slot_id] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetNonOccupied(uint32_t bucket_idx){
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = ~(1 << (bucket_idx % 8));
  this->occupied_[slot_id] &= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = 1 << (bucket_idx % 8);
  return this->readable_[slot_id] & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = 1 << (bucket_idx % 8);
  this->readable_[slot_id] |= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetNonReadable(uint32_t bucket_idx) {
  uint32_t slot_id = bucket_idx / 8;
  uint8_t mask = ~(1 << (bucket_idx % 8));
  this->readable_[slot_id] &= mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint32_t size = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    size++;
  }
  return size == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t taken = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      taken++;
    }
  }
  return taken;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint32_t size = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    size++;
  }
  return size == 0;
}

/**
 * @brief 刷新 bucket_page, 将所有已删除的 key 去除，压缩 bucket
 * 
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Flush() {
//  for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++){
//    // 遍历所有插槽, 将所有被移除的 occupied 设置为 0
//    if(!this->IsReadable(i)) {
//      this->SetNonOccupied(i);
//    }
//  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
