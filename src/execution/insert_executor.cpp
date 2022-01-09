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
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    // 获取对应的 table info
    TableInfo* table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->TableOid());
    TableHeap* table_heap = table_info->table_.get();
    if(!this->plan_->IsRawInsert()) {
        // 不是 raw insert
        return false;
    }else{
        std::vector<std::vector<Value>> raw_values = this->plan_->RawValues();
        if (this->insert_idx < raw_values.size()) {
            std::vector<Value> raw_value = raw_values[this->insert_idx];
            RID rid;
            // 构造要插入的元组
            Tuple tuple = Tuple{raw_value, &table_info->schema_};
            bool res = table_heap->InsertTuple(tuple, &rid, this->exec_ctx_->GetTransaction());
            this->insert_idx++;
            return res;
        }else{
            return false;
        }
    }
}

}  // namespace bustub
