//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    outer_executor(std::move(child_executor)),
    inner_table_info(exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())),
    inner_index_info(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info->name_)),
    inner_index_(inner_index_info->index_.get()){}

void NestIndexJoinExecutor::Init() {
    outer_executor->Init();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) { 
    Tuple outer_tp, inner_tp;
    RID outer_rid;
    const Schema *outer_schema = plan_->OuterTableSchema();
    const Schema *inner_schema = plan_->InnerTableSchema();
    std::vector<RID> inner_result;

    while(outer_executor->Next(&outer_tp, &outer_rid)){
        // If the database already has an index for one of the tables on the join key,
        // it can use that to speed up the comparison
        // 以下注释的做法个人认为不可取，因为这次只是因为我们要连接的那一列刚好在outer_schema的第0列,
        // inner_index_->GetKeyAttrs()才能起到作用，否则无效
        // Tuple key_(outer_tp.KeyFromTuple(*outer_schema, inner_index_info->key_schema_, inner_index_->GetKeyAttrs()));
        //再一次说明不能随便抄代码，要自己真正想明白

        //ref: https://github.com/yzhshiki/bustub_2020/blob/b647284b93/src/execution/nested_index_join_executor.cpp
        const ColumnValueExpression *outer_expr = static_cast<const ColumnValueExpression *>(plan_->Predicate()->GetChildAt(0));
        //子expr中存有其列在outer table的output_schema的列号
        //这里的colId_in_outerOutSchema指的是join后outer表输出的schema
        //而OuterSchema指的是SeqScan扫描的outer表的全schema信息
        auto colId_in_outerOutSchema = outer_expr->GetColIdx();
        auto col_name = plan_->OuterTableSchema()->GetColumn(colId_in_outerOutSchema).GetName();
        //获取outer表的全schema信息中的列号
        auto colId_in_OuterSchema = outer_executor->GetOutputSchema()->GetColIdx(col_name);
        std::vector<uint32_t> key_attrs;
        key_attrs.push_back(colId_in_OuterSchema);
        Tuple key_(outer_tp.KeyFromTuple(*outer_executor->GetOutputSchema(), inner_index_info->key_schema_, key_attrs));
        inner_index_->ScanKey(key_, &inner_result, exec_ctx_->GetTransaction());
        for(auto &inner_rid : inner_result){
            inner_table_info->table_->GetTuple(inner_rid, &inner_tp, exec_ctx_->GetTransaction());
            if(plan_->Predicate()==nullptr ||
                plan_->Predicate()->EvaluateJoin(&outer_tp, plan_->OuterTableSchema(), &inner_tp, plan_->InnerTableSchema()).GetAs<bool>()){
                    std::vector<Value> val(plan_->OutputSchema()->GetColumnCount());
                    std::vector<Column> cols = plan_->OutputSchema()->GetColumns();
                    for(auto i = 0; i < val.size(); i++){
                        if(i<inner_schema->GetColumnCount())
                            val[i] = cols[i].GetExpr()->Evaluate(&inner_tp, inner_schema);
                        else
                            val[i] = cols[i].GetExpr()->Evaluate(&outer_tp, outer_schema);
                    }
                    *tuple = Tuple(val, plan_->OutputSchema());
                    return true;
                }
        }
        
   
    }
    return false; 
}

}  // namespace bustub
