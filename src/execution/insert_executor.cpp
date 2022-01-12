//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
        this->child_executor_ = std::move(child_executor);
    }

void InsertExecutor::Init() {
    this->insert_idx = 0;
    if(!this->plan_->IsRawInsert()){
      this->child_executor_.get()->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    // 获取对应的 table info
    TableInfo* table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
    TableHeap* table_heap = table_info->table_.get();
    if(!this->plan_->IsRawInsert()) {
        // 不是 raw insert
        auto child_executor = this->child_executor_.get();
        Tuple insert_tuple;
        RID insert_rid;
        if(child_executor->Next(&insert_tuple, &insert_rid)){
          table_heap->InsertTuple(insert_tuple, &insert_rid, this->exec_ctx_->GetTransaction());
          auto table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->TableOid());
          auto indexs = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
          for(auto index: indexs){
            index->index_.get()->InsertEntry(insert_tuple, insert_rid, this->GetExecutorContext()->GetTransaction());
          }
          return true;
        }else{
          return false;
        }
    }else{
        std::vector<std::vector<Value>> raw_values = this->plan_->RawValues();
        if (this->insert_idx < raw_values.size()) {
            std::vector<Value> raw_value = raw_values[this->insert_idx];
            // 构造要插入的元组
            const Tuple insert_tuple = Tuple{raw_value, &table_info->schema_};
            // RID insert_rid;
            bool res = table_heap->InsertTuple(insert_tuple, rid, this->exec_ctx_->GetTransaction());
            auto table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->TableOid());
            auto indexs = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
            for(auto index: indexs){
                index->index_.get()->InsertEntry(insert_tuple, *rid, this->GetExecutorContext()->GetTransaction());
            }
            this->insert_idx++;
            return res;
        }else{
            return false;
        }
    }
}

}  // namespace bustub
