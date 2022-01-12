//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  ~HashJoinExecutor(){
    delete this->left_child_.release();
    delete this->right_child_.release();
  }
  
  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  // 一个简单的哈希算法
  size_t Hash(std::string data){
    size_t hash_val = this->hash_fn_(data);
    return hash_val;
    // for(uint32_t i = 0; i < len; i++) {
    //   char val = *(data + i);
    //   hash_val += this->hash_fn_(val);
    // }
    // return hash_val;
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  // 用来构建 hash 关系的容器
  std::unordered_map<size_t, std::vector<Tuple>> ht_{};
  // 哈希函数
  std::hash<std::string> hash_fn_;
  // 用来放置 join 的结果
  std::vector<std::pair<RID, Tuple>> results;
  // 调用 Next 方法时需要记录的 id 号
  size_t tuple_idx;
};

}  // namespace bustub
