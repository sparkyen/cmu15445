//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(page_table_.find(page_id)!=page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    page->pin_count_ += 1;
    //Pin(T) should be called after a page is pinned to a frame in the BufferPoolManager
    if(page->pin_count_==1) replacer_->Pin(frame_id);
    return page;
  }
  if (free_list_.empty() && replacer_->Size() == 0)  return nullptr;
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } 
  else replacer_->Victim(&frame_id);

  // If R is dirty, write it back to the disk.
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    //printf("\n###### %d 's data: %s ######\n", page->page_id_, page->data_);
  }
  // Delete R from the page table and insert P.
  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;
  //Pin(T) should be called after a page is pinned to a frame in the BufferPoolManager
  replacer_->Pin(frame_id);
  
  // Update P's metadata, 
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  //read in the page content from disk
  disk_manager_->ReadPage(page_id, page->data_);
  
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  //return false if the page pin count is <= 0 before this call
  if(page->pin_count_==0) return false;
  page->pin_count_ -= 1;
  //可能其他线程持有Page时给了脏标志，而其他线程的不能因为没有写脏就破坏原有的脏标志
  if(!page->is_dirty_&&is_dirty)
    page->is_dirty_ = is_dirty;
  if(page->pin_count_==0) {
    replacer_->Unpin(frame_id);
    //有三个时机可以刷新到磁盘: 每次unpin后、pin_count=0和不需要其并执行DeletePage
    // if(is_dirty) disk_manager_->WritePage(page_id, page->data_);
  }
  return true; 
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  //FlushPageImpl should flush a page regardless of its pin status.
  if(page_table_.find(page_id)==page_table_.end()) return false;
  Page* page = &pages_[page_table_[page_id]];
  disk_manager_->WritePage(page->page_id_, page->data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  std::scoped_lock lock{latch_};
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    //if a Page object does not contain a physical page, then its page_id must be set to INVALID_PAGE_ID.
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } 
  else {
    if(!replacer_->Victim(&frame_id))
      return nullptr;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page_table_.erase(page->page_id_);

  *page_id = disk_manager_->AllocatePage();
  //Pin(T) should be called after a page is pinned to a frame in the BufferPoolManager
  page_table_[*page_id] = frame_id;
  replacer_->Pin(frame_id);
  // 更新page内的信息
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_table_.find(page_id)==page_table_.end()) return true;
  
  frame_id_t frame_id = page_table_[page_id];
  Page* page = &pages_[frame_id];
  if(page->pin_count_!=0) return false;
  
  //Remove P from the page table
  page_table_.erase(page_id);
  
  //reset its metadata
  //要注意下面if和下段代码执行的先后顺序，否则刷新无效
  if(page->is_dirty_){
    //选择在不需要其并执行DeletePages时刷新到磁盘
    disk_manager_->WritePage(page->page_id_, page->data_);
    //printf("\n###### %d 's data: (%s) ######\n", page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->ResetMemory();

  //return it to the free list.
  free_list_.emplace_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for(size_t i = 0; i < pool_size_; i++){
    Page* page = &pages_[i];
    if(page->page_id_==INVALID_PAGE_ID) continue;
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
}

}  // namespace bustub