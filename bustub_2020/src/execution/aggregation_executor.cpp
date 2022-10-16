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
        aht_.InsertCombine(this->MakeKey(&scan_tp), this->MakeVal(&scan_tp));
    }
    //aht的内容被修改了，这里的aht_iterator_需要重新赋值，否则指向空
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    std::cout << "@AggregationExecutor: Next RUNING" << std::endl;
    while(aht_iterator_!=aht_.End()){
        Tuple agg_tp = Tuple(aht_iterator_.Val().aggregates_, plan_->OutputSchema());
        ++aht_iterator_; 
        //评估having条件是否满足
        if(plan_->GetHaving()==nullptr || 
            plan_->GetHaving()->Evaluate(&agg_tp, plan_->OutputSchema()).GetAs<bool>()){
                *tuple = agg_tp, *rid = tuple->GetRid();
                return true;
            }
        
    }    
    return false; 

}

}  // namespace bustub
