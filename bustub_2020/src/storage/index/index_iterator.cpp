/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, LeafPage *leaf, int idx)
    :buffer_pool_manager_(bpm), leaf(leaf), idx(idx) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { 
    return leaf->GetNextPageId()==INVALID_PAGE_ID && idx == leaf->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    return leaf->GetItem(idx);
}

INDEX_TEMPLATE_ARGUMENTS
/*
 * 最开始没有反应过来是怎么做的，后来搁置段时间重新看时，就明白了：
 * 每次获取当前叶子节点中的信息，获取完后就跳到相邻的下一个叶子节点，直到整层叶子节点的终点
 * 
 */
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    if(idx==leaf->GetSize()-1 && leaf->GetNextPageId()!=INVALID_PAGE_ID){
        Page* next_leaf_page = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());

        //不要忘记在取消其在bufferPool中的引用,但是顺序不要弄错了
        //要在下一行代码前面
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);

        leaf = reinterpret_cast<LeafPage *>(next_leaf_page->GetData());
        idx = 0;
    }
    else idx++;

    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf->GetPageId() == itr.leaf->GetPageId() && idx == itr.idx;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(leaf->GetPageId() == itr.leaf->GetPageId() && idx == itr.idx);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
