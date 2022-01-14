//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  Tuple GenerateTuple() {
      auto out_schema = this->GetOutputSchema();
      auto columns = out_schema->GetColumns();
      std::vector<Value> values;
      for(auto column: columns){
          auto value = column.GetExpr()->Evaluate(&this->iter_->second, out_schema);
          values.push_back(value);
      }
      auto tuple = Tuple(values, this->GetOutputSchema());
      return tuple;
  }

  size_t Hash(Tuple tuple){
      auto out_schema = this->GetOutputSchema();
      auto columns = out_schema->GetColumns();
      size_t hash_val = 0;
      for(auto column: columns){
          auto value = column.GetExpr()->Evaluate(&tuple, out_schema);
          hash_val += this->hash_fn_(value.ToString());
      }
      return hash_val;
  }

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // 经过哈希后存放的 Tuple
  std::unordered_map<size_t, Tuple> ht_;
  // 哈希算法
  std::hash<std::string> hash_fn_;
  // 哈希表的迭代器
  std::unordered_map<size_t, Tuple>::const_iterator iter_;

};
}  // namespace bustub
