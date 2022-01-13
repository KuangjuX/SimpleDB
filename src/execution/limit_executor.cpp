//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      this->plan_ = plan;
      this->child_executor_ = std::move(child_executor);
      this->counts = 0;
}

void LimitExecutor::Init() {
      this->child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
    if(this->counts < this->plan_->GetLimit()) {
        if(this->child_executor_->Next(tuple, rid)) {
            this->counts++;
            return true;
        }
        return false;
    }
    return false;
}

}  // namespace bustub
