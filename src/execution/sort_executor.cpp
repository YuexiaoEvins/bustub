#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_{std::move(child_executor)}{}

void SortExecutor::Init() {
  child_executor_->Init();
  tuple_list_.clear();

  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_executor_->Next(&tmp_tuple,&tmp_rid)){
    tuple_list_.push_back(tmp_tuple);
  }

  auto cmp = [order_bys = plan_->GetOrderBy(),schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b){
    for (const auto &order_key :order_bys) {
      switch (order_key.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareLessThan(order_key.second->Evaluate(&b, schema)))){
            return true;
          }
          if (static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareGreaterThan(order_key.second->Evaluate(&b, schema)))){
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareLessThan(order_key.second->Evaluate(&b, schema)))){
            return false;
          }
          if (static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareGreaterThan(order_key.second->Evaluate(&b, schema)))){
            return true;
          }
          break;
      }
    }
    return false;
  };

  std::sort(tuple_list_.begin(),tuple_list_.end(),cmp);
  cursor_ = tuple_list_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == tuple_list_.end()){
    return false;
  }

  *tuple = *cursor_;
  *rid = tuple->GetRid();
  ++cursor_;
  return true;
}

}  // namespace bustub
