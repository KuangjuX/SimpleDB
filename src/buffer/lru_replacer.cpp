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
    this->frame_lru = new std::list<frame_id_t>;
}

LRUReplacer::~LRUReplacer() {
    delete this->frame_lru;
};

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    if(this->size == 0){
        return false;
    }else {
        this->buf_lock.lock();
        this->size -= 1;
        *frame_id = this->frame_lru->back();
        this->frame_lru->pop_back();
        this->buf_lock.unlock();
        return true;
    }
}

// 在 BufferPoolManager 中的一个页面被 pinned 之后应调用此方法,它
// 应该从 LRUReplacer 中移除包含 pinned 页面的帧
void LRUReplacer::Pin(frame_id_t frame_id) {
    this->buf_lock.lock();
    for(auto item = this->frame_lru->begin(); item != this->frame_lru->end(); item++) {
        if (*item == frame_id) {
            // 发现了对应的 frame_id, 将其从当前位置移到 LRU 的头部
            this->size -= 1;
            this->frame_lru->erase(item);
            // this->frame_lru->push_front(frame_id);
            this->buf_lock.unlock();
            return;
        }
    }
    this->buf_lock.unlock();
}

// 当页面的 pin_count 为 0 时，应调用此方法，此方法应该将
// 包含 unpinned 的页面的帧添加到 LRUReplacer 中
void LRUReplacer::Unpin(frame_id_t frame_id) {
    this->buf_lock.lock();
    for(auto item = this->frame_lru->begin(); item != this->frame_lru->end(); item++) {
        if (*item == frame_id) {
            this->buf_lock.unlock();
            return;
        }
    }
    if(this->frame_lru->size() >= this->page_nums) {
        this->frame_lru->pop_back();
        this->frame_lru->push_front(frame_id);
    }else{
        this->size += 1;
        this->frame_lru->push_front(frame_id);
    }
    this->buf_lock.unlock();
}

size_t LRUReplacer::Size() { return this->size; }

}  // namespace bustub
