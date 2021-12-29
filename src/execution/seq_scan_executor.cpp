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
    TableHeap* table_heap = table_info->table_.get();
    // 通过 TableHeap 构造 TableIterator
    TableIterator table_iterator = table_heap->Begin(this->exec_ctx_->GetTransaction());
    // this->table_iterator = table_iterator;
    this->table_iterator = new TableIterator(table_iterator);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    // 获取 TableInfo
    TableInfo* table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
    // 获取对应的 table_heap
    TableHeap* table_heap = table_info->table_.get();
    while(*this->table_iterator != table_heap->End()){
        Tuple table_tuple = **this->table_iterator;
        this->table_iterator++;
        Value value = this->plan_->GetPredicate()->Evaluate(&table_tuple, this->GetOutputSchema());
        bool res = value.GetAs<bool>();
        printf("%s\n", res ? "true" : "false");
        if (res){
            *tuple = table_tuple;
            *rid = table_tuple.GetRid();
            return true;
        }
    }
    return false;
}

}  // namespace bustub
