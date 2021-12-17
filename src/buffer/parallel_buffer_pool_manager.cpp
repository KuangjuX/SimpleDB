//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  this->num_instances = num_instances;
  this->pool_size = pool_size;
  this->disk_manager = disk_manager;
  this->log_manager = log_manager;
  this->buffer_pool_managers = new BufferPoolManagerInstance*[num_instances];
  // 初始化 BufferPoolManagerInstances
  for(uint32_t i = 0; i < num_instances; i++) {
    BufferPoolManagerInstance** addr = this->buffer_pool_managers + i;
    *addr = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return this->pool_size;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  // return nullptr;
  size_t manager_id = page_id % this->num_instances;
  BufferPoolManagerInstance* buffer_pool_manager = this->buffer_pool_managers[manager_id];
  return buffer_pool_manager;
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  // return nullptr;
  this->latch_.lock();
  BufferPoolManager* buffer_manager = this->GetBufferPoolManager(page_id);
  Page* P = buffer_manager->FetchPage(page_id);
  this->latch_.unlock();
  return P;
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  // return false;
  this->latch_.lock();
  BufferPoolManager* buffer_manager = this->GetBufferPoolManager(page_id);
  bool res = buffer_manager->UnpinPage(page_id, is_dirty);
  this->latch_.unlock();
  return res;
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  // return false;
  this->latch_.lock();
  BufferPoolManager* buffer_manager = this->GetBufferPoolManager(page_id);
  bool res = buffer_manager->FlushPage(page_id);
  this->latch_.unlock();
  return res;
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  // return nullptr;
  this->latch_.lock();
  for(size_t i = 0; i < this->num_instances; i++) {
    BufferPoolManager* buffer_manager = this->buffer_pool_managers[i];
    Page* P = buffer_manager->NewPage(page_id);
    if(P != nullptr) {
      this->latch_.unlock();
      return P;
    }
  }
  this->latch_.unlock();
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  // return false;
  this->latch_.lock();
  BufferPoolManager* buffer_manager = this->GetBufferPoolManager(page_id);
  bool res = buffer_manager->DeletePage(page_id);
  this->latch_.unlock();
  return res;
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  this->latch_.lock();
  for(size_t i = 0; i < this->num_instances; i++) {
    BufferPoolManager* buffer_manager = this->buffer_pool_managers[i];
    buffer_manager->FlushAllPages();
  }
  this->latch_.unlock();
}

}  // namespace bustub
