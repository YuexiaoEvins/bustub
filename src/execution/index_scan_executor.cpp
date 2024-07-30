//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),plan_{plan} {}

void IndexScanExecutor::Init() {
  IndexInfo* index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  auto tx = exec_ctx_->GetTransaction();
  rid_list_.clear();

  if (nullptr != plan_->filter_predicate_){
    const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    std::vector<Value> values;
    values.push_back(right_expr->val_);
    hash_index->ScanKey(Tuple(values,&index_info->key_schema_),&rid_list_,tx);
  }
  cursor_ = rid_list_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::string table_name = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_;
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(table_name)->table_.get();
  while(true){
    if (cursor_ == rid_list_.end()){
      return false;
    }

    *rid = *cursor_;
    auto meta = table_heap->GetTupleMeta(*rid);
    if (meta.is_deleted_){
      continue ;
    }

    *tuple = table_heap->GetTuple(*rid).second;
    cursor_++;

    if (plan_->filter_predicate_ != nullptr) {
      if (plan_->filter_predicate_.get()->Evaluate(tuple,GetOutputSchema()).GetAs<bool>()) {
        return true;
      }
    }

    return true;
  }
}

}  // namespace bustub
