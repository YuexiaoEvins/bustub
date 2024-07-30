#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (nullptr == seq_scan_plan.filter_predicate_){
      return optimized_plan;
    }

    const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    if (cmp_expr == nullptr || cmp_expr->comp_type_ != ComparisonType::Equal){
      return optimized_plan;
    }

    auto column_value_expr = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    if (nullptr == column_value_expr){
      return optimized_plan;
    }

    auto match_index = MatchIndex(seq_scan_plan.table_name_, column_value_expr->GetColIdx());
    auto predicate_key = dynamic_cast<ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
    if (predicate_key != nullptr && match_index.has_value()){
      auto [index_id,index_name] = *match_index;
//      std::vector<AbstractExpressionRef> pred_keys;
//      pred_keys.push_back(std::shared_ptr<AbstractExpression>(cmp_expr->GetChildAt(1)));
      return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, seq_scan_plan.table_oid_, index_id,
                                                 seq_scan_plan.filter_predicate_);
    }

  }

  return optimized_plan;
}

}  // namespace bustub
