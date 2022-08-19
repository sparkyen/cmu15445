//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  //需要使用 get/set 方法，否则使用 private 对象直接 error: page_id_’ is private within this context
  SetPageId(page_id);
  //不要把 SetParentPageId 写成 SetPageId 
  SetParentPageId(parent_id);
  //实际容量为原maxsize-1
  SetMaxSize(max_size-1);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first =  key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
  for(int i = 0; i < GetSize(); i++){
    if(array[i].second==value) return i;
  }
  return -1; 
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  //不需要 assert(index>=0&&index<GetSize())
  //因为其逻辑会在更高层的(调用其的函数)那里受到判断和约束
  //半天找不出bug的教训告诉你不要随便乱抄别人的代码，自己要思索再三
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 * ref: https://github.com/nefu-ljw/database-cmu15445-fall2020/blob/main/src/storage/page/b_plus_tree_internal_page.cpp
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  //从第二个位置开始才会有值出现
  int now_size = GetSize();
  //不需要 assert(now_size>1);
  //若只有一个位置的值说明就在最左边的指针这里 
  /*
    * Lookup含义为：查找now_key应该在哪个value指向的子树中
    * 若找到的位置为i, 则有key(i) <= now_key < key(i+1)
    * 则只需寻找<= now_key的数中最大的一个即可
    * 例如：1 3 9 17 29 31 40 中，给定now_key = 22
  */
  int l = 0, r = now_size-1;
  while(l<r){
    int mid = (l+r+1)/2;
    if(comparator(array[mid].first, key)<=0) l = mid;
    else r = mid-1;
  }
  return array[l].second;
}

/*****************************************************************************
 * INSERTION
 * ref: https://github.com/yixuaz/CMU-15445/blob/master/project2A/b_plus_tree.cpp
 *****************************************************************************/
/*
 * Populate(填充) new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  //插入一直溢出到根节点仍然放不下
  //于是新的根节点同时指向旧的根节点所在的pageID和溢出来部分的pageID
  array[0].second = old_value;
  array[1] = MappingType(new_key, new_value);
  //不要忘记更新page中键值对个数
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int now_index = ValueIndex(old_value);
  //判断是否能找到value==old_value的位置
  //不需要判断是否有足够空间插入新的元素, 在b_plus_tree.cpp层面会判断, 若溢出会进行对该page进行split操作
  int now_size = GetSize();
  assert(now_index!=-1);
  //需要将其他元素往后移
  for(int i = now_size; i >= now_index+2; i--){
    array[i] = array[i-1];
  }
  array[now_index+1] = MappingType(new_key, new_value);
  IncreaseSize(1);
  return now_size+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int start_index = GetMinSize();
  int move_num = GetSize()-start_index;
  std::cout << "@Internal's MoveHalfTo: BEGIN "<< std::endl;
  recipient->CopyNFrom(array+start_index, move_num, buffer_pool_manager);
  std::cout << "@Internal's MoveHalfTo: DONE "<< std::endl;
  IncreaseSize(-move_num);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //将[items, items+size)拷贝到以array+GetSize()所在的地址空间
  std::copy(items, items+size, array+GetSize());
  //更改拷贝后所指向page的parentPageID
  for(int i = 0; i < size; i++){
    std::cout << "@Internal's CopyNFrom: index is "<< GetSize()+i << std::endl;
    Page* page = buffer_pool_manager->FetchPage(ValueAt(GetSize()+i));
    // std::cout << "@Internal's CopyNFrom: now_child's PageID is "<< page->GetPageId() << std::endl;
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    //不要把SetParentPageId写成SetPageId
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
  //不要漏掉page中节点个数的更新
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int maxNum = GetMaxSize();
  for(int i = index; i < maxNum-1; i++)
    array[index] = array[index+1];
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { 
  SetSize(0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  //还需要设置嘛？
  //合并时父节点需要下移
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
