//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
        this->left_child_ = std::move(left_child);
        this->right_child_ = std::move(right_child);
        this->tuple_idx = 0;
    }

void HashJoinExecutor::Init() {
    this->left_child_.get()->Init();
    this->right_child_.get()->Init();
    // 初始化的时候应当建立哈希表
    const ColumnValueExpression* left_join_key_expression = dynamic_cast<const ColumnValueExpression*>(this->plan_->LeftJoinKeyExpression());
    auto left_schema = this->plan_->GetLeftPlan()->OutputSchema();
    Tuple tuple;
    RID rid;
    while(this->left_child_.get()->Next(&tuple, &rid)) {
        // 获得进行 hash join 的 index 的值
        Value value = tuple.GetValue(left_schema, left_join_key_expression->GetColIdx());
        auto data = value.ToString();
        auto hash_val = this->Hash(data);
        if(this->ht_.count(hash_val) == 0){
            std::vector<Tuple> bucket;
            bucket.push_back(tuple);
            this->ht_.insert({hash_val, bucket});
        }
    }

    // 这里实际把 Next() 要做的事情放到 Init() 来做，然后放到
    // 一个成员变量里
    Tuple right_tuple;
    RID right_rid;
    while(this->right_child_.get()->Next(&right_tuple, &right_rid)){
        // this->current_tuple = right_tuple;
        auto right_schema = this->plan_->GetRightPlan()->OutputSchema();
        auto right_join_key_experssion = dynamic_cast<const ColumnValueExpression*>(this->plan_->RightJoinKeyExpression());
        auto value = right_tuple.GetValue(right_schema, right_join_key_experssion->GetColIdx());
        auto data = value.ToString();
        auto hash_val = this->Hash(data);
        if(this->ht_.find(hash_val) != this->ht_.end()) {
            auto bucket = this->ht_.find(hash_val)->second;
            for(auto left_tuple_item: bucket) {
                // left_tuple_item 为存储的 Tuple
                // 此时对其执行 join 操作
                // 也就是根据 out_schema 将其拼接成输出的 Tuple
                
                std::vector<Value> tuple_values;
                // 获取到输出的所有 Column
                // std::vector<Column> columns = this->plan_->OutputSchema()->GetColumns();
                // for(auto column: columns){
                //     auto left_columns = this->plan_->GetLeftPlan()->OutputSchema()->GetColumns();
                //     auto right_columns = this->plan_->GetRightPlan()->OutputSchema()->GetColumns();
                //     // 分别将输出的 column 与左右 column 进行比较并 push 到 tuple_values 中
                //     for(auto left_column: left_columns){
                //         if (left_column.GetExpr() == column.GetExpr()) {
                //             // auto value = left_tuple_item.GetValue()
                //         }
                //     }
                // }
                for(uint32_t column_idx = 0; column_idx < left_schema->GetColumnCount(); column_idx++){
                    auto left_value = left_tuple_item.GetValue(left_schema, column_idx);
                    tuple_values.push_back(left_value);
                }
                for(uint32_t column_idx = 0; column_idx < right_schema->GetColumnCount(); column_idx++){
                    auto right_value = right_tuple.GetValue(right_schema, column_idx);
                    tuple_values.push_back(right_value);
                }
                auto out_tuple = Tuple(tuple_values, this->GetOutputSchema());
                auto out_rid = out_tuple.GetRid();
                this->results.push_back({out_rid, out_tuple});
            }
        }
    }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
    // 这里需要不断遍历 right_child 每条 tuple 并进行 hash 与
    // 构建哈希表中的进行连接,需要考虑一下如何判断连接的 tuple 是否
    // 该调用 Next() 方法
    // printf("[Debug] Call Next.\n");
    if(this->tuple_idx >= this->results.size()) {
        return false;
    }else{
        auto pair = this->results[tuple_idx++];
        *rid = pair.first;
        *tuple = pair.second;
        return true;
    }
}

}  // namespace bustub
