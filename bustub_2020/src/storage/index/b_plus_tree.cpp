//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { 
  return root_page_id_==INVALID_PAGE_ID; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page* leaf_page = FindLeafPage(key);
  std::cout << "@GetValue: FindLeafPage DONE" << std::endl;
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  ValueType tmpValue;
  bool existed = leaf->Lookup(key, &tmpValue, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  std::cout << "@GetValue: pinCount is " << leaf_page->GetPinCount() << std::endl;

  if(!existed) return false;

  result->push_back(tmpValue);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
  std::cout << "\n@Insert: Begin to Insert" << std::endl;
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page* page = buffer_pool_manager_->NewPage(&root_page_id_);
  if(page==nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  LeafPage* leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  std::cout << "@StartNewTree: leaf is RootPage ? " << leaf_page->GetParentPageId() << std::endl;
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ << " has " << leaf_page->GetSize() 
  << " elements" << std::endl;
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ << " insert " << key.ToString() << " " << value.ToString() << std::endl;
  leaf_page->Insert(key, value, comparator_);
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ 
  << " has " << leaf_page->GetSize() 
  << " & isEmpty(): " << IsEmpty()
  << std::endl;
  
  UpdateRootPageId(true);

  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  std::cout << "@StartNewTree: pinCount is " << page->GetPinCount() << std::endl;
  
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  std::cout << "@InsertIntoLeaf: insert " << key.ToString() << " " << value.ToString();
  //1. find the right leaf page as insertion target
  //???FindLeafPage????????????Fetch???Pin??????
  Page* leaf_page = FindLeafPage(key);
  if(leaf_page==nullptr) return false;

  //2. look through leaf page to see whether insert key exist or not
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  std::cout << "@InsertIntoLeaf: leaf is RootPage ? " << leaf->GetParentPageId() << std::endl;
  ValueType tmpValue;
  //2.1 If exist, return immdiately
  //since we only support unique key, 
  //if user try to insert duplicate keys return false
  if(leaf->Lookup(key, &tmpValue, comparator_)){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    std::cout << "@Insert: pinCount is "<< leaf_page->GetPinCount() << std::endl;
    return false;
  }
  //2.2  otherwise insert entry
  leaf->Insert(key, value, comparator_);
  std::cout << "@InsertIntoLeaf: leaf's PageID is " << leaf->GetPageId() << std::endl;
  if(leaf->GetSize()<leaf->GetMaxSize()){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    std::cout << "@Insert: pinCount is "<< leaf_page->GetPinCount() << std::endl;
    return true;
  }
  //2.2.1 split if necessary
  LeafPage* new_leaf = Split(leaf);
  std::cout << "@InsertIntoLeaf: SPLIT DONE" << std::endl;
  //leaf -> next_leaf >>> leaf -> new_leaf -> next_leaf
  //???????????????
  //???????????????SetNextPageId??????SetPageId
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());
  
  KeyType middle_key = new_leaf->KeyAt(0);
  std::cout << "@InsertIntoLeaf: leaf is RootPage ? " << leaf->IsRootPage() << std::endl;
  std::cout << "@InsertIntoLeaf: new_leaf's PageID is " << new_leaf->GetPageId() << std::endl;
  //????????????transaction
  InsertIntoParent(leaf, middle_key, new_leaf, transaction);
   std::cout << "@InsertIntoLeaf: leaf " << leaf->GetPageId() 
   << " with new_leaf " << new_leaf->GetPageId() << std::endl;

  //3. ??????????????????????????????????????????
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  std::cout << "@Insert: pinCount is "<< leaf_page->GetPinCount() << std::endl;
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
// 0. Using template N to represent either internal page or leaf page.
N *BPLUSTREE_TYPE::Split(N *node) {
  std::cout << "@Split: Begin to split node " << node->GetPageId() << std::endl;
  //1. ask for new page from buffer pool manager
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
  std::cout << "@Split: new_node's PageID is " << new_page_id << std::endl;
  //1.2 throw an "out of memory" exception if returned value is nullptr
  if(new_page==nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());
  if(node->IsLeafPage()){
    std::cout << "@Split: node " << node->GetPageId() << " is LeafNode" << std::endl;
    LeafPage* leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage* new_leaf = reinterpret_cast<LeafPage *>(new_node);
    //??????????????????????????????
    new_leaf->Init(new_page_id, leaf->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
    // std::cout << "@Split: new_leaf's PageID is " << new_leaf->GetPageId() << std::endl;
    std::cout << "@Split: DONE" << std::endl;
  }
  else {
    std::cout << "@Split: node " << node->GetPageId() << " is InternalNode" << std::endl;
    InternalPage* internal = reinterpret_cast<InternalPage *>(node);
    InternalPage* new_internal = reinterpret_cast<InternalPage *>(new_node);
    //??????????????????????????????
    //???????????????internal->GetParentPageId()??????internal->GetPageId()
    new_internal->Init(new_page_id, internal->GetParentPageId(), internal_max_size_);
    std::cout << "@Split: new_internal's PageID is " << new_internal->GetPageId() << std::endl;
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
    std::cout << "@Split: MoveHalfTo DONE "<< std::endl;
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
   std::cout << "@InsertIntoParent: BEGIN" << std::endl;
   std::cout << "@InsertIntoParent: insert " << key.ToString() << " " << new_node->GetPageId() << std::endl;
  //1. old_node????????????????????????????????????????????????
  if(old_node->IsRootPage()){
    std::cout << "@InsertIntoParent: old_node " << old_node->GetPageId() << " is RootNode" << std::endl;
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
    //????????????????????????????????????????????????
    if(new_page==nullptr){
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    root_page_id_ = new_page_id;
    // update root page id in header page
    UpdateRootPageId(false);

    // std::cout << "@InsertIntoParent: root_node's PageID is " << root_page_id_ << std::endl;
    InternalPage* root = reinterpret_cast<InternalPage *>(new_page->GetData());
    //?????????????????????
    root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    std::cout << "@InsertIntoParent: root_node's PageID is " << root->GetPageId() << std::endl;
    //??????????????????????????????PopulateNewRoot()?????????InsertNodeAfter()
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    std::cout << "@InsertIntoParent: old_node's PageID is " << old_node->GetPageId()
    << " & new_node's PageID is " << new_node->GetPageId() << std::endl;
    // ??????old_node???new_node????????????
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    std::cout << "@InsertIntoParent: old_node & new_node's ParentPageID is " << old_node->GetParentPageId() << std::endl;
    // std::cout << "@InsertIntoParent: root_node's PageID is " << root->GetPageId() << std::endl;
    //????????????????????????
    //?????????????????????????????????old_node???new_node?????????
    //??????InsertIntoParent()????????????????????????
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    
    return;
  }


  //2.1. find the parent page of old_node
  Page* parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  //2.2. insert in right position
  //???????????????page_id????????????Page*
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Caution: ???????????????new_node????????????,?????????split()?????????????????????
  // new_node->SetParentPageId(old_node->GetParentPageId());
  std::cout << "@InsertIntoParent: old_node & new_node's ParentPageID is " << new_node->GetParentPageId() << std::endl;
  std::cout << "@InsertIntoParent: parent_node's PageID is " << parent_node->GetPageId() << std::endl;
  //2.3. deal with split recursively if necessary.
  // you should correctly perform split 
  // if insertion triggers current number of key/value pairs after insertion equals to max_size
  if(parent_node->GetSize()>=parent_node->GetMaxSize()){
    // std::cout << "@InsertIntoParent: parent_node's PageID is " << parent_node->GetPageId() << std::endl;
    InternalPage* new_parent_node = Split(parent_node);
    std::cout << "@InsertIntoParent: new_parent_node's PageID is " << new_parent_node->GetPageId() << std::endl;
    KeyType middle_key = new_parent_node->KeyAt(0);
    InsertIntoParent(parent_node, middle_key, new_parent_node);
    buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::cout << "@Remove: Begin to remove " << key.ToString() << std::endl;
  //1. find the right leaf page as deletion target
  Page* leaf_page = FindLeafPage(key);
  std::cout << "@Remove: $ pinCount is "<< leaf_page->GetPinCount() << std::endl;
  //1.1 If current tree is empty / couldn't find target, return immdiately.
  if(leaf_page==nullptr) return;
  //2. delete entry from leaf page
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int now_size = leaf->GetSize();
  if(now_size==leaf->RemoveAndDeleteRecord(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return;
  }

  bool leaf_should_delete = false;
  //2.1 deal with redistribute or merge if necessary.
  if(leaf->GetSize()<leaf->GetMinSize()){
    std::cout << "@Remove: [leaf " << leaf->GetPageId() 
    << "] size is " << leaf->GetSize() << std::endl;
    leaf_should_delete = CoalesceOrRedistribute(leaf, transaction);
  }

  std::cout << "@Remove: $$ pinCount is "<< leaf_page->GetPinCount() << std::endl;
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if(leaf_should_delete){
    std::cout << "@Remove: pinCount is "<< leaf_page->GetPinCount() << std::endl;
    assert(buffer_pool_manager_->DeletePage(leaf_page->GetPageId()));
  }
    
  return;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  std::cout << "@CoalesceOrRedistribute: node is RootPage? " << node->IsRootPage() << std::endl;
  //0. ?????????????????????
  if(node->IsRootPage()){
    //?????????????????????????????????????????????
    bool root_should_delete = AdjustRoot(node);
    return root_should_delete;
  } 
  //1. ????????????node???parent
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int node_index = parent->ValueIndex(node->GetPageId());
  //2.1 ??????node???parent??????????????????????????????????????????
  std::cout << "@CoalesceOrRedistribute: node is "  
  << node->GetPageId() << " in parent " << node->GetParentPageId() 
  << " with index " << node_index << std::endl;
  N* prev_node;
  Page* prev_page;
  if(node_index==0) {
    node_index = 1;
    prev_page = buffer_pool_manager_->FetchPage(parent->ValueAt(1));
    prev_node = reinterpret_cast<N *>(prev_page->GetData());
    std::swap(node, prev_node);
  }
  //2.2 ??????????????????prev_node
  else {
    std::cout << "@CoalesceOrRedistribute: this line run " << parent->ValueAt(node_index-1) << std::endl;
    prev_page = buffer_pool_manager_->FetchPage(parent->ValueAt(node_index-1));
    std::cout << "@CoalesceOrRedistribute: pre_page's ID is  " << prev_page->GetPageId() << std::endl;
    prev_node = reinterpret_cast<N *>(prev_page->GetData());
    std::cout << "@CoalesceOrRedistribute: prev_node is " << prev_node->GetPageId() << std::endl;
  }
  //3.1 ???node?????????????????????prev_node????????????
  //????????? < ????????? <=
  //node????????????
  if(node->GetSize()+prev_node->GetSize()<node->GetMaxSize()){
    bool parent_should_delete = Coalesce(&prev_node, &node, &parent, node_index, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    if(parent_should_delete) //delete this page
      assert(buffer_pool_manager_->DeletePage(parent->GetPageId()));
    //?????????prev_page?????????prev_node
    buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true);
    //node should be deleted
    return true;
  }
  // 3.2 ??????????????????
  //Redistribute?????????parent???????????????
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  Redistribute(prev_node, node, node_index);
  buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), true);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  std::cout << "@Coalesce: from node " << (*node)->GetPageId() << 
  " to prev_node " << (*neighbor_node)->GetPageId() << std::endl;
  if((*node)->IsLeafPage()){
    LeafPage* leaf = reinterpret_cast<LeafPage *>(*node);
    LeafPage* prev_leaf = reinterpret_cast<LeafPage *>(*neighbor_node);
    //??????????????????????????????MoveAllTo?????????
    //NOTE: ?????????????????????????????????????????????????????????????????????????????????????????????
    leaf->MoveAllTo(prev_leaf);
  }
  else{
    KeyType middle_key = (*parent)->KeyAt(index);
    InternalPage* internal = reinterpret_cast<InternalPage *>(*node);
    InternalPage* prev_internal = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal->MoveAllTo(prev_internal, middle_key, buffer_pool_manager_);
  }
  //??????parent????????????
  (*parent)->Remove(index);

  std::cout << "@Coalesce: parent's size is " << (*parent)->GetSize() << std::endl;
  //deal with coalesce or redistribute recursively if necessary.
  //??????????????????(*parent)?????????????????????, ??????????????????????????????????????????
  //?????????GetMinSize()????????????RootPage?????????
  // ref: https://gist.github.com/Zrealshadow/b6c4fee458881cc8d5796ff719f9b650
  if((*parent)->GetSize()>=(*parent)->GetMinSize() && !(*parent)->IsRootPage()){
    //parent node no deletion
    return false;
  }
  // parent node should be deleted
  return CoalesceOrRedistribute(*parent, transaction);
  
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  std::cout << "@Redistribute: node " << node->GetPageId() << "(parent index " << index <<
  ") with prev_node " << neighbor_node->GetPageId() << std::endl;
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  KeyType parent_middle_key;
  //???????????????????????????????????????????????????
  if(node->IsLeafPage()){
    LeafPage* leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage* prev_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    if(prev_leaf->GetSize() > leaf->GetSize())
      prev_leaf->MoveLastToFrontOf(leaf);
    else 
      leaf->MoveFirstToEndOf(prev_leaf);
    //??????parent??????index??????
    //NOTE: ?????????????????????????????????middle_key???parent_middle_key?????????
    parent_middle_key = (leaf->GetItem(0)).first;
  }
  else {
    InternalPage* internal = reinterpret_cast<InternalPage *>(node);
    InternalPage* prev_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    KeyType middle_key = parent->KeyAt(index);
    if(prev_internal->GetSize() > internal->GetSize()){
      prev_internal->MoveLastToFrontOf(internal, middle_key, buffer_pool_manager_);
    }
    else {
      internal->MoveFirstToEndOf(prev_internal, middle_key, buffer_pool_manager_);
    } 
    //??????parent??????index??????
    parent_middle_key = internal->KeyAt(0);
  }

  //??????Unpin
  std::cout << "@Redistribute: parent's middle_key is " << parent_middle_key << std::endl;
  parent->SetKeyAt(index, parent_middle_key);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);

  return;
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
  //1.1 root page still has one last child
  if(!old_root_node->IsLeafPage()&&old_root_node->GetSize()==1) {
    InternalPage* internal = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = internal->RemoveAndReturnOnlyChild();
    // update root page id in header page
    UpdateRootPageId(false);
    // update parentPageId in new rootPage
    Page* leaf_page = buffer_pool_manager_->FetchPage(root_page_id_);
    LeafPage* root = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    std::cout << "@AdjustRoot: root goes down to node " << root_page_id_ << endl;
    //root page should be deleted
    return true;
  }
  //1.2 deleted the last element in whole b+ tree
  else if(old_root_node->GetSize()==0){
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    return true; 
  }
  //no deletion
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { 
  //??????KeyType????????????
  Page* left_leaf_page = FindLeafPage(KeyType(), true);
  LeafPage* leaf = reinterpret_cast<LeafPage *>(left_leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, 0); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  Page* leaf_page = FindLeafPage(key);
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int idx = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, idx); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  Page* leaf_page = FindLeafPage(KeyType(), true);
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  while(leaf->GetNextPageId()!=INVALID_PAGE_ID){
    leaf_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  }
  //end?????????idx????????????????????????????????????????????????
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, leaf->GetSize()); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * leftMost: ?????????????????????
 */
INDEX_TEMPLATE_ARGUMENTS
//leftMost???????????????false, ????????????b_plus_tree.h???
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //????????????????????????????????????INVALID_PAGE_ID
  if(IsEmpty()) return nullptr;
  //???????????????????????????
  Page* origin_page = buffer_pool_manager_->FetchPage(root_page_id_);
  std::cout << "@FindLeafPage: $ pinCount is "<< origin_page->GetPinCount() << std::endl;
  assert(origin_page!=nullptr);
  //??????????????????leafPage
  /*
    Each B+Tree leaf/internal page corresponds to the content
     (i.e., the data_ part) of a memory page fetched by buffer pool.
    So every time you try to read or write a leaf/internal page, 
    you need to first fetch the page from buffer pool using 
    its unique page_id, then reinterpret cast to either a leaf or an internal page, 
    and unpin the page after any writing or reading operations.
  */
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(origin_page->GetData());
  //?????????parent_page_id???????????????tree_page->GetPageId()??????Unpin root_page
  page_id_t parent_page_id = tree_page->GetPageId(), child_page_id;
  while(!tree_page->IsLeafPage()){
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(tree_page);
    if(leftMost) child_page_id = internal_page->ValueAt(0);
    else child_page_id = internal_page->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(parent_page_id, false);

    parent_page_id = child_page_id;
    origin_page = buffer_pool_manager_->FetchPage(parent_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(origin_page->GetData());
  }
  //????????????unpin origin_page???????????????????????????page??????
  std::cout << "@FindLeafPage: $$ pinCount is "<< origin_page->GetPinCount() << std::endl;
  return origin_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 * ????????????????????????????????????
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  //modified by sparkyen(origin: INT_)
  std::string internal_prefix("INTERNAL_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        //Add by sparkyen
        << "now_size=" << leaf->GetSize()
        << ",max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      //Add by sparkyen
      if(i!=0&&i%15==0) out << "</TR>\n<TR>";
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "now_size=" << inner->GetSize()
        << ",max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      //Add by sparkyen
      out << "(" << inner->KeyAt(i) << ", "
        << inner->ValueAt(i) << ")";

      // if (i > 0) {
      //   out << inner->KeyAt(i);
      // } else {
      //   out << " ";
      // }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
