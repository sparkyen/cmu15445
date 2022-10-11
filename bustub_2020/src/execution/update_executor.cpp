//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
    child_executor_(std::move(child_executor)),
    index_list_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)){}

void UpdateExecutor::Init() {
    child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    while(child_executor_->Next(tuple, rid)){
        Tuple new_tuple = GenerateUpdatedTuple(*tuple);
        if(table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())){
            if(index_list_.empty()) continue;
            for(auto &index : index_list_){
                Tuple old_key_(tuple->KeyFromTuple(table_info_->schema_, 
                    index->key_schema_, index->index_->GetKeyAttrs()));
                Tuple new_key_(new_tuple.KeyFromTuple(table_info_->schema_, 
                    index->key_schema_, index->index_->GetKeyAttrs()));
                //( (val, key_schema), rid)
                index->index_->DeleteEntry(old_key_, *rid, exec_ctx_->GetTransaction());
                index->index_->InsertEntry(new_key_, *rid, exec_ctx_->GetTransaction());
            }
        }
        else {
            throw Exception("Failed to UPDATE");
        }
    }
    return false; 
}
}  // namespace bustub
