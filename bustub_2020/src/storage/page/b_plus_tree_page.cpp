//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const { 
    return page_type_ == IndexPageType::LEAF_PAGE;
}
bool BPlusTreePage::IsRootPage() const { 
    return parent_page_id_ == INVALID_PAGE_ID;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    page_type_ = page_type;
}
IndexPageType BPlusTreePage::GetPageType() const { 
    return page_type_; 
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { 
    return size_; 
}
void BPlusTreePage::SetSize(int size) {
    size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
    size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const { 
    return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * With n = 4 in our example B+-tree, each leaf must contain at least 2 values, and at most 3 values.
 */
int BPlusTreePage::GetMinSize() const { 
    //若考虑根节点的特殊情况，AdjustRoot()和Coalesce的实现会更加优美
    //ref: https://gist.github.com/Zrealshadow/b6c4fee458881cc8d5796ff719f9b650
    
    //需要向上取整, 但是实际容量为max_size-1，便不需要+1了
    return (max_size_)/2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { 
    return parent_page_id_; 
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    //注意细节，不要把parent_page_id写成parent_page_id_
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { 
    return page_id_;
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
    page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { 
    lsn_ = lsn; 
}

}  // namespace bustub
