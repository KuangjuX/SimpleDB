//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {}

void SeqScanExecutor::Init() {
    // 获取 TableInfo
    TableInfo* table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
    // 获取对应的 table_heap
    std::unique_ptr<TableHeap> table_heap = std::move(table_info->table_);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    return false; 
}

}  // namespace bustub
