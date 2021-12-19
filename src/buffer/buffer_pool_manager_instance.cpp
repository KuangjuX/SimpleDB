//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// 刷新页面，除非是在 Pinned 状态下
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // return false;
  this->latch_.lock();
  for(size_t i = 0; i < this->pool_size_; i++) {
    Page* P = &this->pages_[i];
    P->RLatch();
    if(P->GetPageId() == page_id && P->GetPinCount() == 0) {
      this->disk_manager_->WritePage(page_id, P->GetData());
      this->latch_.unlock();
      P->RUnlatch();
      return true;
    }
    P->RUnlatch();
  }
  this->latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  this->latch_.lock();
  for(size_t i = 0; i < this->pool_size_; i++) {
    Page* P = &this->pages_[i];
    P->RLatch();
    if(P->GetPinCount() == 0) {
      this->disk_manager_->WritePage(P->GetPageId(), P->GetData());
      this->latch_.unlock();
      P->RUnlatch();
      return;
    }
    P->RUnlatch();
  }
  this->latch_.unlock();
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  this->latch_.lock();
  // 首先从 free_list 中查找空闲的 frame, 若 free_list 中没找到就从 replacer 中查找，
  // 都没找到则返回空指针
  frame_id_t frame_id;
  if(this->free_list_.size() > 0){
    // 从 free_list 中获取 frame_id
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
    // 将其从空闲链表中取出来，应该需要 Pin
    this->replacer_->Pin(frame_id);
  }else if(this->replacer_->Victim(&frame_id)) {
    // 从 replacer 中获取 frame_id
  }else{
    // 没有找到返回空指针
    this->latch_.unlock();
    return nullptr;
  }
  // 获取给定帧号的页指针
  Page* P = &this->pages_[frame_id];
  // 上写锁，避免多线程同时访问
  P->WLatch();
  // 分配物理页
  page_id_t new_page_id = this->AllocatePage();
  // 更新 P 的元数据并且清空内存
  P->page_id_ = new_page_id;
  P->pin_count_ = 1;
  P->ResetMemory();
  // 设置输出 page_id 参数
  *page_id = P->page_id_;
  P->WUnlatch();
  this->latch_.unlock();
  return P;
}


Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // return nullptr;
  this->latch_.lock();
  for(size_t i = 0; i < this->pool_size_; i++) {
    Page* P = &this->pages_[i];
    if(P->page_id_ == page_id) {
      // 找到了内存中对应的物理页，将其固定并返回指针
      P->pin_count_ += 1;
      this->latch_.unlock();
      return P;
    }
  }
  // 物理页不存在内存中, 从 free list 或者 replacement 中找到对应的空闲的帧
  frame_id_t frame_id;
  if(this->free_list_.size() > 0) {
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
    this->replacer_->Pin(frame_id);
  }else if(this->replacer_->Victim(&frame_id)) {
    // 从 Replacement 中获取 frame_id
  }else{
    this->latch_.unlock();
    return nullptr;
  }
  Page* R = &this->pages_[frame_id];
  // 为替换页上写锁
  R->WLatch();
  if(R->IsDirty()) {
    // 当被替换的页是脏的则将页内存写回磁盘
    this->disk_manager_->WritePage(R->GetPageId(), R->GetData());
  }
  // 设置被替换页的元数据
  R->page_id_ = page_id;
  R->pin_count_ = 1;
  // 将该页的数据从磁盘中读进来
  this->disk_manager_->ReadPage(page_id, R->GetData());
  // 为替换页解除写锁
  R->WUnlatch();
  this->latch_.unlock();
  return R;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  // return false;
  this->latch_.lock();
  for(size_t i = 0; i < this->pool_size_; i++){
    Page* P = &this->pages_[i];
    P->WLatch();
    if(P->GetPageId() == page_id){
      if(P->GetPinCount() > 0) {
        // Pin 大于 0，不能直接删除，返回 false
        this->latch_.unlock();
        P->WUnlatch();
        return false;
      }else{
        // 此时页面是 Unpinned 状态, 删除页面并更新物理页面上的元数据
        // this->DeallocatePage(page_id);
        this->free_list_.push_back(i);
        this->replacer_->Unpin(i);
        P->ResetMemory();
        P->page_id_ = INVALID_PAGE_ID;
        P->is_dirty_ = false;
        P->pin_count_ = 0;
        // 解锁
        P->WUnlatch();
        this->latch_.unlock();
        return true;
      }
    }
    P->WUnlatch();
  }
  // 没有找到，直接返回true
  this->latch_.unlock();
  return true;
}

// id_dirty 参数追踪当某页面被 Pinned 的时候是否该页面被更改，
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  this->latch_.lock();
  for(size_t i = 0; i < this->pool_size_; i++) {
    Page* P = &this->pages_[i];
    P->WLatch();
    if(P->GetPageId() == page_id && P->GetPinCount() > 0) {
      P->pin_count_ -= 1;
      if(P->pin_count_ == 0) {
        // 获取到对应的 page_id
        if(is_dirty){
          this->disk_manager_->WritePage(page_id, P->GetData());
        }
        this->DeallocatePage(page_id);
        P->ResetMemory();
        P->page_id_ = INVALID_PAGE_ID;
        P->is_dirty_ = false;
        this->free_list_.push_back(i);
        this->replacer_->Unpin(i);

        P->WUnlatch();
        this->latch_.unlock();
        return true;
      }else{
        P->WUnlatch();
        this->latch_.unlock();
        return true;
      }
    }
    P->WUnlatch();
  }
  this->latch_.unlock();
  return false;
}

// 分配页面，并返回页号
page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

// 验证分配的 page_id 是否符合要求
void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
