//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

// 初始化 LRUReplacer
LRUReplacer::LRUReplacer(size_t num_pages) {
  this->page_nums = num_pages;
  this->size = 0;
  this->lru_list = new std::list<frame_id_t>;
  this->lru_map = new std::map<frame_id_t, size_t>;
}

LRUReplacer::~LRUReplacer() {
  delete this->lru_list;
  delete this->lru_map;
};

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (this->size == 0) {
    return false;
  } else {
    this->buf_lock.lock();
    // 将 frame_id 从队尾删除, 并将其从 map 中删除
    this->size -= 1;
    *frame_id = this->lru_list->back();
    this->lru_list->pop_back();
    this->lru_map->erase(*frame_id);

    this->buf_lock.unlock();
    return true;
  }
}

// 在 BufferPoolManager 中的一个页面被 pinned 之后应调用此方法,它
// 应该从 LRUReplacer 中移除包含 pinned 页面的帧
void LRUReplacer::Pin(frame_id_t frame_id) {
  this->buf_lock.lock();
  if (this->lru_map->find(frame_id) != this->lru_map->end() && this->size > 0) {
    this->lru_map->erase(frame_id);
    this->lru_list->remove(frame_id);
    this->size -= 1;
  }
  this->buf_lock.unlock();
}

// 当页面的 pin_count 为 0 时，应调用此方法，此方法应该将
// 包含 unpinned 的页面的帧添加到 LRUReplacer 中
void LRUReplacer::Unpin(frame_id_t frame_id) {
  this->buf_lock.lock();
  if (this->lru_map->find(frame_id) != this->lru_map->end()) {
    // 在 LRU 有当前 frame_id, 此时不做任何改变
  } else {
    if (this->size < this->page_nums) {
      this->lru_map->insert(std::pair<frame_id_t, size_t>(frame_id, 0));
      this->lru_list->push_front(frame_id);
      this->size += 1;
    } else {
      // 当容量不够时，首先驱逐最近最少使用的 frame 然后将当前 frame 加入到 List 和 Map 中
      frame_id_t evlict_frame_id = this->lru_list->back();
      this->lru_list->pop_back();
      this->lru_map->erase(evlict_frame_id);
      this->lru_list->push_front(frame_id);
      this->lru_map->insert(std::pair<frame_id_t, size_t>(frame_id, 0));
    }
  }
  this->buf_lock.unlock();
}

size_t LRUReplacer::Size() { return this->size; }

}  // namespace bustub
