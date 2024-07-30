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
    : AbstractExecutor(exec_ctx),plan_{plan},child_executor_{std::move(child_executor)} {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_called_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_called_){
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  auto tx = exec_ctx_->GetTransaction();
  auto index_array = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple child_tuple{};

  int affected_row = 0;
  while(child_executor_->Next(&child_tuple,rid)) {
    auto insert_result = table_heap->InsertTuple(
        TupleMeta{tx->GetTransactionTempTs(), false},
        child_tuple,
        exec_ctx_->GetLockManager(),
        tx,
        table_info->oid_
    );

    if (*rid == RID() && insert_result.has_value()){
      *rid = insert_result.value();
    }

    for (auto index_info :index_array) {
      index_info->index_->InsertEntry(
          child_tuple.KeyFromTuple(table_info->schema_,index_info->key_schema_,index_info->index_->GetKeyAttrs()),
          *rid,tx);
    }
    affected_row++;
    *rid = RID();
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, affected_row);

  *tuple = Tuple(values,&GetOutputSchema());
  has_called_ = true;
  return true;
}

}  // namespace bustub
