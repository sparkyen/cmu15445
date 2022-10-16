//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    pos(0) {}

void LimitExecutor::Init() {
    child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) { 
    while(child_executor_->Next(tuple, rid)){
        if(pos<plan_->GetOffset()){
            pos = pos+1;
            //这里不能写return true而是要写continue，否则会返回空的tuple
            //ref: https://github.com/yzhao001/15445-2020FALL/blob/0624328941/src/execution/limit_executor.cpp
            continue;
        }
        else if(pos<plan_->GetLimit()+plan_->GetOffset()){
            pos = pos+1;
            return true;
        }
    }
    return false; 
}

}  // namespace bustub
