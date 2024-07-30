//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {
}

void UpdateExecutor::Init() {
  has_called_ = false;
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_called_){
    return false;
  }

  auto table_heap = table_info_->table_.get();
  auto tx = exec_ctx_->GetTransaction();
  auto index_array = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  int affected_row = 0;
  Tuple old_tuple;

  while(child_executor_->Next(&old_tuple,rid)){
    std::vector<Value> updated_values;
    updated_values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (auto expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&old_tuple,child_executor_->GetOutputSchema()));
    }

    auto new_tuple = Tuple(updated_values,&child_executor_->GetOutputSchema());

    bool updated_result = table_heap->UpdateTupleInPlace(
        TupleMeta{tx->GetTransactionTempTs(),false},
        new_tuple,*rid);

    if (!updated_result){
      continue ;
    }

    for (auto index : index_array) {
      index->index_->DeleteEntry(
          old_tuple.KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs()),
          *rid,tx);
      index->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_,index->key_schema_,index->index_->GetKeyAttrs()),
          *rid,tx);
    }

    affected_row++;
  }

  std::vector<Value> result;
  result.reserve(GetOutputSchema().GetColumnCount());
  result.emplace_back(INTEGER,affected_row);
  *tuple = Tuple(result,&GetOutputSchema());
  has_called_ = true;
  return true;
}

}  // namespace bustub
