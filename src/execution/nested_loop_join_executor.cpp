//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  RID left_rid;
  left_has_next = left_executor_->Next(&left_tuple_,&left_rid);
}

auto NestedLoopJoinExecutor::LeftJoinRemainedLeftTuple(Tuple *left_tuple) ->Tuple  {
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
auto NestedLoopJoinExecutor::InnerJoinTuple(Tuple *left_tuple,Tuple *right_tuple) ->Tuple {
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

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID left_rid;
  RID right_rid;

  while (true){
    if(!left_has_next) {
      return false;
    }

    if(!right_executor_->Next(&right_tuple,&right_rid)) {
      if(plan_->GetJoinType() == JoinType::LEFT && !current_left_has_done){
        *tuple = LeftJoinRemainedLeftTuple(&left_tuple_);
        *rid = tuple->GetRid();

        current_left_has_done = true;
        return true;
      }

      right_executor_->Init();
      left_has_next = left_executor_->Next(&left_tuple_,&left_rid);
      current_left_has_done = false;
      continue ;
//      return true;
    }

    auto is_match = plan_->Predicate()->EvaluateJoin(&left_tuple_,left_executor_->GetOutputSchema(),
                                                     &right_tuple,right_executor_->GetOutputSchema());
    if(!is_match.IsNull() && is_match.GetAs<bool>()) {
      *tuple = InnerJoinTuple(&left_tuple_,&right_tuple);
      *rid = tuple->GetRid();

      current_left_has_done = true;
      return true;
    }

  }
}

}  // namespace bustub
