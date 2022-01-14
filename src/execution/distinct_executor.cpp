//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      this->plan_ = plan;
      this->child_executor_ = std::move(child_executor);
}

void DistinctExecutor::Init() {
      this->child_executor_->Init();
      Tuple tuple;
      RID rid;
      while(this->child_executor_->Next(&tuple, &rid)) {
          auto hash_val = this->Hash(tuple);
          if(this->ht_.count(hash_val) == 0){
            this->ht_.insert({hash_val, tuple});
          }
      }
      this->iter_ = this->ht_.cbegin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
    if(this->iter_ != this->ht_.cend()) {
      *tuple = this->GenerateTuple();
      *rid = tuple->GetRid();
      ++this->iter_;
      return true;
    }
    return false;
}

}  // namespace bustub
