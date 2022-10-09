//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)){}

void InsertExecutor::Init() {
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    index_info_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    if(!plan_->IsRawInsert()){
        child_executor_->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    if(plan_->IsRawInsert()){
        std::vector<std::vector<Value>> vals = plan_->RawValues();
        for(auto &val : vals){
            Tuple tuple_(val, &table_info_->schema_);
            if(table_info_->table_->InsertTuple(tuple_, rid, exec_ctx_->GetTransaction())){
                for(auto &index : index_info_){
                    // Tuple key_(tuple_.KeyFromTuple(table_info_->schema_, index->key_schema_))
                }
            }
        }
    }
    return false; 
}

}  // namespace bustub
