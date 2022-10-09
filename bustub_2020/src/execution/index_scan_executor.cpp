//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
    plan_(plan) {}

void IndexScanExecutor::Init() {
    index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
    auto index_ = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
    index_iter = index_->GetBeginIterator();
    end_ = index_->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) { 
    while(index_iter!=end_){
        //index_iter的*运算符重载返回的是MappingType
        *rid = (*index_iter).second, ++index_iter;
        if(!table_info->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction())){
            throw std::out_of_range("Failed to get tuple");
        }

        if(plan_->GetPredicate()==nullptr || 
            plan_->GetPredicate()->Evaluate(tuple, &table_info->schema_).GetAs<bool>()){
                const Schema* output_schema = plan_->OutputSchema();
                std::vector<Value> val(output_schema->GetColumnCount());
                std::vector<Column> cols = output_schema->GetColumns();
                for(uint i = 0; i < val.size(); i++){
                    //cols[i].GetExpr()的具体解释看grading_executor_test::MakeOutputSchema()
                    val[i] = cols[i].GetExpr()->Evaluate(tuple, &table_info->schema_);
                }
                *tuple = Tuple(val, output_schema);
                return true;
            }

    }
    return false; 
}

}  // namespace bustub
