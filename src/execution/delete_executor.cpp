//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void DeleteExecutor::Init() {
  has_called_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_called_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_list = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto tx = exec_ctx_->GetTransaction();
  auto table_heap = table_info->table_.get();

  Tuple child_tuple{};
  RID emit_rid;
  int affected_row = 0;

  while(child_executor_->Next(&child_tuple,&emit_rid)){
    table_heap->UpdateTupleMeta(TupleMeta{tx->GetTransactionTempTs(),true},emit_rid);
    for (auto index_info : index_list) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(table_info->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),
          emit_rid,tx);
    }
    affected_row++;
  }
  std::vector<Value> result_row;
  result_row.reserve(GetOutputSchema().GetColumnCount());
  result_row.emplace_back(INTEGER,affected_row);
  *tuple = Tuple(result_row,&GetOutputSchema());

  has_called_ = true;
  return true;
}

}  // namespace bustub
