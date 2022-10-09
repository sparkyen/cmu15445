//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx) ,
    plan_(plan),
    table_mata_data_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
    table_iter(table_mata_data_->table_->Begin(exec_ctx_->GetTransaction())){}

void SeqScanExecutor::Init() {
    std::cout << "@SeqScanExecutor: INIT invoked " << std::endl;
    //table_iter要在init中或者构造函数中赋值，否则不断的从头开始
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    while(table_iter!=table_mata_data_->table_->End()){
        // std::cout << table_iter->ToString(&table_mata_data_->schema_) << std::endl;
        *tuple = *(table_iter++);
        
        // std::cout << "@SeqScanExecutor: tuple is " << (*tuple).ToString(&table_mata_data_->schema_) << std::endl;
        /*
        SQL 中的谓词是真/假条件，例如在 WHERE 子句或 HAVING 子句中。
        所以 WHERE a = b 是一个谓词：它可以是 TRUE 或 FALSE。
        因此，您在执行计划中看到的是对这些条件的评估
        */
        if(plan_->GetPredicate()==nullptr || 
            plan_->GetPredicate()->Evaluate(tuple, &table_mata_data_->schema_).GetAs<bool>()){
                // std::cout << "@SeqScanExecutor: Evaluate" << (plan_->GetPredicate()->Evaluate(tuple, &table_mata_data_->schema_)).ToString() << std::endl;
                *rid = tuple->GetRid();
                const Schema* output_schema = plan_->OutputSchema();
                std::vector<Value> val(output_schema->GetColumnCount());
                std::vector<Column> cols = output_schema->GetColumns();
                for(uint i = 0; i < val.size(); i++){
                    val[i] = cols[i].GetExpr()->Evaluate(tuple, &table_mata_data_->schema_);
                    std::cout << "@SeqScanExecutor: val[i] is " << val[i].ToString() << std::endl;
                }
                *tuple = Tuple(val, output_schema);
                return true;
            }
        
    }
    
    return false; 
}
    
}  // namespace bustub
