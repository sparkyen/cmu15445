//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_plan_((SeqScanPlanNode *)plan_->GetLeftPlan()),
    right_plan_((SeqScanPlanNode *)plan_->GetRightPlan()),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)),
    left_table_info(exec_ctx_->GetCatalog()->GetTable(left_plan_->GetTableOid())),
    right_table_info(exec_ctx_->GetCatalog()->GetTable(right_plan_->GetTableOid())) {}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) { 
    std::cout << "@NestedLoopJoinExecutor: BEGIN" << std::endl;
    //这里要定义对象而不是指针，否则写入数据时候找不到地址会报错
    Tuple tuple1, tuple2;
    RID rid1, rid2;
    const Schema *left_output_schema = left_plan_->OutputSchema();
    const Schema *right_output_schema = right_plan_->OutputSchema();
    std::cout << "@NestedLoopJoinExecutor: BEGIN 2" << std::endl;

    //while分开写，而不是用&&连接
    while(left_executor_->Next(&tuple1, &rid1)){
        std::cout << "NestedLoopJoinExecutor: left execution DONE" << std::endl;
        while(right_executor_->Next(&tuple2, &rid2)){
            std::cout << "@NestedLoopJoinExecutor: right execution DONE" << std::endl;
            //貌似直接用cols[i].GetExpr()->EvaluateJoin直接返回会更简单、一步到位，但是毕竟下面是自己写的
            //而关于cols[i].GetExpr()中原本expr哪里来的参见Grading_executor_test::SimpleNestedLoopJoinTest中注释
            if(plan_->Predicate()==nullptr ||
                plan_->Predicate()->EvaluateJoin(&tuple1, &(left_table_info->schema_),
                    &tuple2, &(right_table_info->schema_)).GetAs<bool>()){
                        std::vector<Value> val(left_output_schema->GetColumnCount()+right_output_schema->GetColumnCount());
                        std::vector<Column> left_cols = left_output_schema->GetColumns();
                        std::vector<Column> right_cols = right_output_schema->GetColumns();
                        for(auto i = 0; i < left_output_schema->GetColumnCount(); i++)
                            val[i] = left_cols[i].GetExpr()->Evaluate(&tuple1, &left_table_info->schema_);
                        auto now_size = left_output_schema->GetColumnCount();
                        //这里也要注意数组下标的使用不要超出下标了
                        for(auto i = 0; i < right_output_schema->GetColumnCount(); i++)
                            val[now_size+i] = right_cols[i].GetExpr()->Evaluate(&tuple2, &right_table_info->schema_);
                        *tuple = Tuple(val, plan_->OutputSchema());
                        return true;
                }
        }
    } 
    return false; 
}

}  // namespace bustub
