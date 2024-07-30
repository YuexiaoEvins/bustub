#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_{std::move(child_executor)} {}

void TopNExecutor::Init() {
  auto cmp = [order_bys = plan_->GetOrderBy(),schema = GetOutputSchema()]
      (const Tuple& a,const Tuple& b){
        for (auto order_key :order_bys) {
          switch (order_key.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if(static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareLessThan(order_key.second->Evaluate(&b,schema)))){
                return true;
              }
              if(static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareGreaterThan(order_key.second->Evaluate(&b,schema)))){
                return false;
              }
              break;
            case OrderByType::DESC:
              if(static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareGreaterThan(order_key.second->Evaluate(&b,schema)))){
                return true;
              }
              if(static_cast<bool>(order_key.second->Evaluate(&a,schema).CompareLessThan(order_key.second->Evaluate(&b,schema)))){
                return false;
              }
              break;
          }
        }
      return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

  Tuple child_tuple;
  RID child_rid;
  while(child_executor_->Next(&child_tuple,&child_rid)){
    pq.push(child_tuple);
    if (pq.size() > plan_->GetN()){
      pq.pop();
    }
  }

  while(!pq.empty()){
    tuple_list_.push_back(pq.top());
    pq.pop();
  }

  std::reverse(tuple_list_.begin(),tuple_list_.end());
  cursor_ = tuple_list_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ == tuple_list_.end()){
    return false;
  }
  *tuple = *cursor_;
  *rid = tuple->GetRid();
  ++cursor_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuple_list_.size(); };

}  // namespace bustub
