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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    this->plan_ = plan;
}

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
    TableIterator table_heap_end = table_heap->End();
    if(*this->table_iterator != table_heap_end){
        // 获取该 table_iterator 的 tuple
        Tuple table_tuple = **this->table_iterator;
        // 将当前的 table_iterator 指向下一个元组
        ++(*this->table_iterator);
        const AbstractExpression* predicate = this->plan_->GetPredicate();
        Value value;
        if(predicate == nullptr) {
          *tuple = table_tuple;
          *rid = table_tuple.GetRid();
          return true;
        }else{
          value = predicate->Evaluate(&table_tuple, this->GetOutputSchema());
        }
        bool res = value.GetAs<bool>();
        if (res){
            *tuple = table_tuple;
            *rid = table_tuple.GetRid();
//            printf("[Debug] Seq Scan ridL %lu\n", rid->Get());
            return true;
        }else{
            return false;
        }
    }
    return false;
}

}  // namespace bustub
