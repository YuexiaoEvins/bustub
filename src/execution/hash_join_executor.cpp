//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_{std::move(left_child)},
      right_executor_{std::move(right_child)}{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto HashJoinExecutor::MakeHashJoinLeftKey(Tuple *left_tuple) -> HashJoinKey{
  std::vector<Value> values;
  for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
    values.emplace_back(expr->Evaluate(left_tuple, left_executor_->GetOutputSchema()));
  }
  return {values};
}
auto HashJoinExecutor::MakeHashJoinRightKey(Tuple *right_tuple) -> HashJoinKey{
  std::vector<Value> values;
  for (const auto &expr : plan_->RightJoinKeyExpressions()) {
    values.emplace_back(expr->Evaluate(right_tuple, right_executor_->GetOutputSchema()));
  }
  return {values};
}

auto HashJoinExecutor::LeftJoinRemainedLeftTuple(Tuple *left_tuple) -> Tuple{
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(),i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->output_schema_->GetColumn(i).GetType()));
  }
  return Tuple{values,&GetOutputSchema()};
}

auto HashJoinExecutor::InnerJoinTuple(Tuple *left_tuple,Tuple* right_tuple) ->Tuple{
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(),i));
  }

  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(),i));
  }

  return Tuple{values,&GetOutputSchema()};
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  left_has_next_ = left_executor_->Next(&left_tuple_,&left_rid_);

  jht_ = std::make_unique<SimpleHashJoinHashTable>();
  Tuple right_tuple;
  RID right_rid;
  while (right_executor_->Next(&right_tuple,&right_rid)) {
    jht_->InsertKey(MakeHashJoinRightKey(&right_tuple),right_tuple);
  }

  auto left_hash_key = MakeHashJoinLeftKey(&left_tuple_);
  // 在哈希表中查找与左侧元组匹配的右侧元组
  right_tuple_list_ = jht_->GetValue(left_hash_key);
  if (right_tuple_list_ != nullptr){
    jht_right_tuple_list_cursor_ = right_tuple_list_->begin();
    current_left_has_done_ = true;
  } else {
    current_left_has_done_ = false;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true){
    if(!left_has_next_) {
      return false;
    }


    if (right_tuple_list_ != nullptr && jht_right_tuple_list_cursor_ != right_tuple_list_->end()){
      auto right_tuple = *jht_right_tuple_list_cursor_;
      *tuple = InnerJoinTuple(&left_tuple_,&right_tuple);
      *rid = tuple->GetRid();
      ++jht_right_tuple_list_cursor_;
      return true;
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !current_left_has_done_){
      *tuple = LeftJoinRemainedLeftTuple(&left_tuple_);
      *rid = tuple->GetRid();
      current_left_has_done_ = true;
      return true;
    }

    left_has_next_ = left_executor_->Next(&left_tuple_,&left_rid_);
    // 重置右边匹配的元组，以及更新迭代器
    auto left_hash_key = MakeHashJoinLeftKey(&left_tuple_);
    // 在哈希表中查找与左侧元组匹配的右侧元组
    right_tuple_list_ = jht_->GetValue(left_hash_key);
    if (right_tuple_list_ != nullptr){
      jht_right_tuple_list_cursor_ = right_tuple_list_->begin();
      current_left_has_done_ = true;
    } else {
      current_left_has_done_ = false;
    }
  }
}

}  // namespace bustub
