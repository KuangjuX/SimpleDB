//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      this->plan_ = plan;
      this->child_executor_ = std::move(child_executor);
      this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());

}

void DeleteExecutor::Init() {
  this->child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple delete_tuple;
  RID delete_rid;
  if(this->child_executor_->Next(&delete_tuple, &delete_rid)) {
    this->table_info_->table_->ApplyDelete(delete_rid, this->exec_ctx_->GetTransaction());
    return true;
  }
  return false;
}

}  // namespace bustub
