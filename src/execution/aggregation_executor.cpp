//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
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
      aht_{plan->GetAggregates(), plan->GetAggregateTypes()},
      aht_iterator_(aht_.Begin()){}


void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;

  child_->Init();

  // 将查询的所有 tuple hash 后插入到 aht 中
  while (child_->Next(&tuple, &rid)){
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();

}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if(this->aht_iterator_ != aht_.End()) {
    auto values = this->aht_iterator_.Val().aggregates_;
//    auto out_tuple = Tuple(values, this->GetOutputSchema());
    auto having = this->plan_->GetHaving();
    if(having == nullptr ||
        having->EvaluateAggregate(this->aht_iterator_.Key().group_bys_, this->aht_iterator_.Val().aggregates_).GetAs<bool>()){
      *tuple = this->GenerateOutputTuple();
      *rid = tuple->GetRid();
      ++this->aht_iterator_;
      return true;
    }
    ++this->aht_iterator_;
    return this->Next(tuple, rid);
  }
  return false;
}

Tuple AggregationExecutor::GenerateOutputTuple() {
  std::vector<Value> values;
  for (const auto &col: GetOutputSchema()->GetColumns()){
    values.push_back(col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
  }
  return Tuple(values, GetOutputSchema());
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
