//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
      this->plan_ = plan;
      this->left_executor_ = std::move(left_executor);
      this->right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
    this->left_executor_->Init();
    this->right_executor_->Init();
}

// Stupied Algorithm
bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple left_tuple;
    RID left_rid;
    Tuple right_tuple;
    RID right_rid;
    auto left_schema = this->plan_->GetLeftPlan()->OutputSchema();
    auto right_schema = this->plan_->GetRightPlan()->OutputSchema();
    if(this->left_executor_->Next(&left_tuple, &left_rid)) {
      if(this->right_executor_->Next(&right_tuple, &right_rid)) {
        auto predicate = this->plan_->Predicate();
        auto value = predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        auto allow_join = value.GetAs<bool>();
        if(allow_join) {
          // 这里需要根据 out_schema 拼接两个 tuple
          std::vector<Value> tuple_values;
          for(uint32_t column_idx = 0; column_idx < left_schema->GetColumnCount(); column_idx++){
            auto left_value = left_tuple.GetValue(left_schema, column_idx);
            tuple_values.push_back(left_value);
          }
          for(uint32_t column_idx = 0; column_idx < right_schema->GetColumnCount(); column_idx++){
            auto right_value = right_tuple.GetValue(right_schema, column_idx);
            tuple_values.push_back(right_value);
          }
          auto out_tuple = Tuple(tuple_values, this->GetOutputSchema());
          *tuple = out_tuple;
          *rid = out_tuple.GetRid();
//          printf("[Debug] nested loop join executor return\n");
          return true;
        }
        return this->Next(tuple, rid);
      }
      this->right_executor_->Init();
      return this->Next(tuple, rid);
    }
    return false;
}

}  // namespace bustub
