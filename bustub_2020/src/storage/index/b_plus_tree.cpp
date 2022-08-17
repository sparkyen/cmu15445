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
  //不要忘记实现了这个函数哦
  //顺便说一句可能打完球回来就发现问题了
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
  //不需要写一长串递归寻找，可以复用FindLeafPage()
  Page* leaf_page = FindLeafPage(key);
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  ValueType tmpValue;
  bool existed = leaf->Lookup(key, &tmpValue, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

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
  std::cout << "@Insert: Begin to Insert" << endl;
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
  std::cout << "@StartNewTree: leaf is RootPage ? " << leaf_page->GetParentPageId() << endl;
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ << " has " << leaf_page->GetSize() 
  << " elements" << endl;
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ << " insert " << key.ToString() << " " << value.ToString() << endl;
  leaf_page->Insert(key, value, comparator_);
  std::cout << "@StartNewTree: root_page_id_ " << root_page_id_ 
  << " has " << leaf_page->GetSize() 
  << " & isEmpty(): " << IsEmpty()
  << endl;
  
  UpdateRootPageId(true);

  buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
  std::cout << "@InsertIntoLeaf: insert " << key.ToString() << " " << value.ToString() << endl;
  //1. find the right leaf page as insertion target
  //在FindLeafPage时已经在Fetch中Pin过了
  Page* leaf_page = FindLeafPage(key);
  if(leaf_page==nullptr) return false;

  //2. look through leaf page to see whether insert key exist or not
  LeafPage* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  std::cout << "@InsertIntoLeaf: leaf is RootPage ? " << leaf->GetParentPageId() << endl;
  ValueType tmpValue;
  //2.1 If exist, return immdiately
  //since we only support unique key, 
  //if user try to insert duplicate keys return false
  if(leaf->Lookup(key, &tmpValue, comparator_)){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  //2.2  otherwise insert entry
  leaf->Insert(key, value, comparator_);
  if(leaf->GetSize()<=leaf->GetMaxSize()){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  //2.2.1 split if necessary
  LeafPage* new_leaf = Split(leaf);
  std::cout << "@InsertIntoLeaf: SPLIT DONE" << endl;
  //leaf -> next_leaf >>> leaf -> new_leaf -> next_leaf
  //顺序要注意
  //并且不要把SetNextPageId写成SetPageId
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());
  
  KeyType middle_key = new_leaf->KeyAt(0);
  std::cout << "@InsertIntoLeaf: leaf is RootPage ? " << leaf->IsRootPage() << endl;
  std::cout << "@InsertIntoLeaf: new_leaf's PageID is " << new_leaf->GetPageId() << endl;
  //需要传入transaction
  InsertIntoParent(leaf, middle_key, new_leaf, transaction);
   std::cout << "@InsertIntoLeaf: leaf " << leaf->GetPageId() 
   << " with new_leaf " << new_leaf->GetPageId() << endl;

  //3. 使用完后记得取消对页面的引用
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
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
  std::cout << "@Split: Begin to split node " << node->GetPageId() << endl;
  //1. ask for new page from buffer pool manager
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
  std::cout << "@Split: new_node's PageID is " << new_page_id << endl;
  //1.2 throw an "out of memory" exception if returned value is nullptr
  if(new_page==nullptr){
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  N *new_node = reinterpret_cast<N *>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());
  if(node->IsLeafPage()){
    std::cout << "@Split: node " << node->GetPageId() << " is LeafNode" << endl;
    LeafPage* leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage* new_leaf = reinterpret_cast<LeafPage *>(new_node);
    //使用之前需要先初始化
    new_leaf->Init(new_page_id, leaf->GetParentPageId(), leaf_max_size_);
    leaf->MoveHalfTo(new_leaf);
    // std::cout << "@Split: new_leaf's PageID is " << new_leaf->GetPageId() << endl;
    std::cout << "@Split: DONE" << endl;
  }
  else {
    std::cout << "@Split: node " << node->GetPageId() << " is InternalNode" << endl;
    InternalPage* internal = reinterpret_cast<InternalPage *>(node);
    InternalPage* new_internal = reinterpret_cast<InternalPage *>(new_node);
    //使用之前需要先初始化
    //但是不要把internal->GetParentPageId()写成internal->GetPageId()
    new_internal->Init(new_page_id, internal->GetParentPageId(), internal_max_size_);
    std::cout << "@Split: new_internal's PageID is " << new_internal->GetPageId() << endl;
    internal->MoveHalfTo(new_internal, buffer_pool_manager_);
    std::cout << "@Split: MoveHalfTo DONE "<< endl;
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
   std::cout << "@InsertIntoParent: BEGIN" << endl;
  //1. old_node是根结点，那么整棵树直接升高一层
  if(old_node->IsRootPage()){
    std::cout << "@InsertIntoParent: old_node " << old_node->GetPageId() << " is RootNode" << endl;
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
    root_page_id_ = new_page_id;
    // std::cout << "@InsertIntoParent: root_node's PageID is " << root_page_id_ << endl;
    InternalPage* root = reinterpret_cast<InternalPage *>(new_page->GetData());
    //先初始化再使用
    root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    std::cout << "@InsertIntoParent: root_node's PageID is " << root->GetPageId() << endl;
    //对于新建的根节点使用PopulateNewRoot()而不是InsertNodeAfter()
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    std::cout << "@InsertIntoParent: old_node's PageID is " << old_node->GetPageId()
    << " & new_node's PageID is " << new_node->GetPageId() << endl;
    // 修改old_node和new_node的父指针
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    std::cout << "@InsertIntoParent: old_node & new_node's ParentPageID is " << old_node->GetParentPageId() << endl;
    // std::cout << "@InsertIntoParent: root_node's PageID is " << root->GetPageId() << endl;
    //不要忘记写脏标志
    //另外不需要在这里取消对old_node和new_node的引用
    //调用InsertIntoParent()的上层函数会负责
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    // update root page id in header page
    UpdateRootPageId(false);

    return;
  }


  //2.1. find the parent page of old_node
  Page* parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  //2.2. insert in right position
  //插入的值是page_id，而不是Page*
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Caution: 可以不修改new_node的父指针,因为在split()已经初始化过了
  // new_node->SetParentPageId(old_node->GetParentPageId());
  std::cout << "@InsertIntoParent: old_node & new_node's ParentPageID is " << new_node->GetParentPageId() << endl;
  std::cout << "@InsertIntoParent: parent_node's PageID is " << parent_node->GetPageId() << endl;
  //2.3. deal with split recursively if necessary.
  if(parent_node->GetSize()>parent_node->GetMaxSize()){
    // std::cout << "@InsertIntoParent: parent_node's PageID is " << parent_node->GetPageId() << endl;
    InternalPage* new_parent_node = Split(parent_node);
    std::cout << "@InsertIntoParent: new_parent_node's PageID is " << new_parent_node->GetPageId() << endl;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  return false;
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
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
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * leftMost: 在最左边的意思
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //返回的是指针类型，而不是INVALID_PAGE_ID
  if(IsEmpty()) return nullptr;
  //从根节点开始出发，
  Page* origin_page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(origin_page!=nullptr);
  //直到找到目标leafPage
  /*
    Each B+Tree leaf/internal page corresponds to the content
     (i.e., the data_ part) of a memory page fetched by buffer pool.
    So every time you try to read or write a leaf/internal page, 
    you need to first fetch the page from buffer pool using 
    its unique page_id, then reinterpret cast to either a leaf or an internal page, 
    and unpin the page after any writing or reading operations.
  */
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(origin_page->GetData());
  page_id_t parent_page_id, child_page_id;
  while(!tree_page->IsLeafPage()){
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(tree_page);
    if(leftMost) child_page_id = internal_page->ValueAt(0);
    else child_page_id = internal_page->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(parent_page_id, false);

    parent_page_id = child_page_id;
    origin_page = buffer_pool_manager_->FetchPage(parent_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(origin_page->GetData());
  }
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
 * 读入的文件一定要放对路径
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
  std::string internal_prefix("INT_");
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
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
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
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
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
