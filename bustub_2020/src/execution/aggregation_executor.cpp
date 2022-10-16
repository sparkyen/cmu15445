//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_(std::move(child)),
    aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    std::cout << "@AggregationExecutor: INIT" << std::endl;
    child_->Init();
    Tuple scan_tp;
    RID scan_rid;
    while(child_->Next(&scan_tp, &scan_rid)){
        // std::cout << "@AggregationExecutor: scan RUNING" << std::endl;
        //group_bys联合作为键值
        aht_.InsertCombine(this->MakeKey(&scan_tp), this->MakeVal(&scan_tp));
    }
    //aht的内容被修改了，这里的aht_iterator_需要重新赋值，否则指向空
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    std::cout << "@AggregationExecutor: Next RUNING" << std::endl;
    while(aht_iterator_!=aht_.End()){
        std::cout << "@AggregationExecutor: Generate Tuple" << std::endl;
        //ref: https://github.com/ysj1173886760/db_project/blob/4dd467d88b/src/execution/aggregation_executor.cpp
        const Schema *outSchema = plan_->OutputSchema();
        std::vector<Column> cols = outSchema->GetColumns();
        auto key = aht_iterator_.Key().group_bys_;
        auto val = aht_iterator_.Val().aggregates_;
        // Tuple agg_tp = Tuple(aht_iterator_.Val().aggregates_, plan_->OutputSchema());
        ++aht_iterator_; 
        //评估having条件是否满足
        //在 SQL 中增加 HAVING 子句原因是，WHERE 关键字无法与合计函数一起使用
        //HAVING子句可以让我们筛选成组后的对各组数据筛选，而WHERE子句在聚合前先筛选记录。
        //需要根据EvaluateAggregate选择输出的信息是aggregates中的还是group_bys中的
        if(plan_->GetHaving()==nullptr || 
            plan_->GetHaving()->EvaluateAggregate(key, val).GetAs<bool>()){
                std::vector<Value> result(outSchema->GetColumnCount());
                for (uint i = 0; i < result.size(); i++) {
                    result[i] = cols[i].GetExpr()->EvaluateAggregate(key, val);
                }
                *tuple = Tuple(result, outSchema), *rid = tuple->GetRid();
                return true;
            }
        
    }    
    return false; 

}

}  // namespace bustub
