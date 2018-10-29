#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_IMPL_INIT_H_
#define MICA_TRANSACTION_TRANSACTION_IMPL_INIT_H_

#include <iostream>

namespace mica {
namespace transaction {
template <class StaticConfig>
Transaction<StaticConfig>::Transaction(Context<StaticConfig>* ctx)
    : ctx_(ctx), began_(false), pretty_name_map(nullptr),
      checktime_abort_hh(nullptr), checktime_inconsistent_hh(nullptr) {
  last_commit_time_ = 0;

  access_history_.reserve(StaticConfig::kAccessBucketRootCount);

  consecutive_commits_ = 0;

  if (StaticConfig::kAbortHH) {
    if (checktime_abort_hh == nullptr || checktime_inconsistent_hh == nullptr) {
      assert(checktime_abort_hh == nullptr && checktime_inconsistent_hh == nullptr);
      auto irm = Accounter::new_irm();
      pretty_name_map = irm;
      checktime_abort_hh = new Accounter(StaticConfig::kAbortHHSize, irm);
      checktime_inconsistent_hh = new Accounter(StaticConfig::kAbortHHSize, irm);
    }
  }
}

template <class StaticConfig>
Transaction<StaticConfig>::~Transaction() {
  if (began_) abort();

  if (StaticConfig::kAbortHH) {
    print_hh_abort_diagnostics();
    delete checktime_abort_hh;
    delete checktime_inconsistent_hh;
    delete pretty_name_map;
  }
}

}
}

#endif
