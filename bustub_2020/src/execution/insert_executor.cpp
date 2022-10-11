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
    index_list = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
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
                if(index_list.empty()) continue;
                /*
                You will want to look up table and index information during executor construction, 
                and insert into index(es) if necessary during execution.
                */
                for(auto &index : index_list){
                    //KeyFromTuple可以从一个table的tuple构造一个index entry的key tuple
                    //一个index的keyAttrs是一个数组，表明了table的tuple中的列号
                    Tuple key_(tuple_.KeyFromTuple(table_info_->schema_, 
                        index->key_schema_, index->index_->GetKeyAttrs()));
                    // (const Tuple &key, RID rid, Transaction *transaction)
                    index->index_->InsertEntry(key_, *rid, exec_ctx_->GetTransaction());
                }
                
            }
            else {
                throw Exception("Failed to INSERT");
            }
        }
    }
    /*
    有两种类型的插入:
    1. 直接插入(只有插入语句)
    2. 非直接插入(从子语句中获取要插入的值)。
    例如INSERT INTO empty_table2 
    SELECT colA, colB FROM test_1 WHERE colA > 500需要先执行后面的select语句在进行insert。
    */
    else {
        while(child_executor_->Next(tuple, rid)){
            //返回的是单个tuple
            if(table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())){
                if(index_list.empty()) continue;
                for(auto &index : index_list){
                    Tuple key_(tuple->KeyFromTuple(table_info_->schema_, 
                        index->key_schema_, index->index_->GetKeyAttrs()));
                    index->index_->InsertEntry(key_, *rid, exec_ctx_->GetTransaction());
                }
            }
            else {
                throw Exception("Failed to INSERT");
            }
        }
    }
    return false; 
}

}  // namespace bustub
