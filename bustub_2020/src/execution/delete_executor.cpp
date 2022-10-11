//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
    index_list_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
    child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    while(child_executor_->Next(tuple, rid)){
        /*
        You only need to get RID from the child executor 
        and call MarkDelete (src/include/storage/table/table_heap.h) to make it invisable. 
        The deletes will be applied when transaction commits.
        */
        if(table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())){
            if(index_list_.empty()) continue;
            for(auto &index_info : index_list_){
                Tuple key_(tuple->KeyFromTuple(table_info_->schema_, 
                    index_info->key_schema_, index_info->index_->GetKeyAttrs()));
                index_info->index_->DeleteEntry(key_, *rid, exec_ctx_->GetTransaction());
            }
        }
        else{
            throw Exception("Failed to DELETE");
        }
    }
    return false; 
}

}  // namespace bustub
