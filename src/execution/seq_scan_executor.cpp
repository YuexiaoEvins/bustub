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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_{plan} {

}

void SeqScanExecutor::Init() {
  auto table_heap = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(true){
    if (table_iter_->IsEnd()) {
      return false;
    }
    auto tuple_meta = table_iter_->GetTuple().first;
    *rid = table_iter_->GetRID();
    *tuple = table_iter_->GetTuple().second;
    ++*table_iter_;

    if (tuple_meta.is_deleted_){
      continue ;
    }

    if (plan_->filter_predicate_ != nullptr) {
      if (plan_->filter_predicate_.get()->Evaluate(tuple,GetOutputSchema()).GetAs<bool>()) {
        return true;
      }
      continue ;
    }

    return true;
  }
}

}  // namespace bustub
