#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_IMPL_OPERATION_H_
#define MICA_TRANSACTION_TRANSACTION_IMPL_OPERATION_H_

namespace mica {
namespace transaction {
template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::new_row(RAH& rah, Table<StaticConfig>* tbl,
                                        uint16_t cf_id, uint64_t row_id,
                                        bool check_dup_access,
                                        uint64_t data_size,
                                        const DataCopier& data_copier) {
  assert(began_);

  assert(!peek_only_);

  // new_row() requires explicit data sizes.
  assert(data_size != kDefaultWriteDataSize);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  // This rah must not be in use.
  if (rah) return false;

  if (cf_id == 0) {
    if (row_id != kNewRowID) return false;

    row_id = ctx_->allocate_row(tbl);
    if (row_id == static_cast<uint64_t>(-1)) {
      // TODO: Use different stats counter.
      if (StaticConfig::kCollectExtraCommitStats) {
        abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
        abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
      }
      return false;
    }
  } else {
    // Non-zero column family must supply a valid row ID.
    if (row_id == kNewRowID) return false;
  }

  auto head = tbl->head(cf_id, row_id);

  auto write_rv =
      ctx_->allocate_version_for_new_row(tbl, cf_id, row_id, head, data_size);
  if (write_rv == nullptr) {
    // Not enough memory.
    if (cf_id == 0) ctx_->deallocate_row(tbl, row_id);
    return false;
  }

  write_rv->older_rv = nullptr;
  write_rv->wts = ts_;
  write_rv->rts.init(ts_);
  write_rv->status = RowVersionStatus::kPending;

  if (!data_copier(cf_id, write_rv, nullptr)) {
    // Copy failed.
    ctx_->deallocate_version(write_rv);
    if (cf_id == 0) ctx_->deallocate_row(tbl, row_id);
    return false;
  }

  // Prefetch the whole row because it is typically written with new data.
  // {
  //   auto addr = reinterpret_cast<const char*>(write_rv);
  //   auto max_addr = addr + version_size;
  //   for (; addr < max_addr; addr += 64)
  //     __builtin_prefetch(reinterpret_cast<const void*>(addr), 1, 0);
  // }

  if (check_dup_access) {
    auto it = access_history_.find({tbl, row_id, cf_id});
    // new rows must haven't been accessed before
    assert(it == access_history_.end());

    auto ins_res = access_history_.insert({{tbl, row_id, cf_id}, access_size_});
    assert(ins_res.second);
  }

  // assert(access_size_ < StaticConfig::kMaxAccessSize);
  if (access_size_ >= StaticConfig::kMaxAccessSize) {
    printf("too large access\n");
    assert(false);
  }
  iset_idx_[iset_size_++] = access_size_;

  rah.valid_ = true;
  rah.set_item_ = &accesses_[access_size_];
  accesses_[access_size_] = {access_size_, 0,     RowAccessState::kNew,
                             tbl,          cf_id, row_id,
                             head,         head,  write_rv,
                             nullptr /*, ts_*/};

  access_size_++;

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::prefetch_row(Table<StaticConfig>* tbl,
                                             uint16_t cf_id, uint64_t row_id,
                                             uint64_t off, uint64_t len) {
  assert(began_);

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  auto head = tbl->head(cf_id, row_id);
  __builtin_prefetch(head, 0, 0);

  if (StaticConfig::kInlinedRowVersion && tbl->inlining(cf_id) && len > 0) {
    size_t addr =
        (reinterpret_cast<size_t>(head->inlined_rv->data) + off) & ~size_t(63);
    if ((reinterpret_cast<size_t>(head) & ~size_t(63)) == addr) addr += 64;
    size_t max_addr =
        (reinterpret_cast<size_t>(head->inlined_rv->data) + off + len - 1) |
        size_t(63);
    for (; addr <= max_addr; addr += 64)
      __builtin_prefetch(reinterpret_cast<void*>(addr), 0, 0);
  }
}

template <class StaticConfig>
bool Transaction<StaticConfig>::peek_row(RAH& rah, Table<StaticConfig>* tbl,
                                         uint16_t cf_id, uint64_t row_id,
                                         bool check_dup_access, bool read_hint,
                                         bool write_hint) {
  assert(began_);
  if (rah) return false;

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  // Use an access item if it already exists.
  if (check_dup_access) {
    auto it = access_history_.find({tbl, row_id, cf_id});
    if (it != access_history_.end()) {
        // found an access item, use it
        int idx = it->second;
        assert(idx < access_size_);
        rah.add_set_item(accesses_, idx);
        return true;
    }
  }

  auto head = tbl->head(cf_id, row_id);
  if (StaticConfig::kInlinedRowVersion && StaticConfig::kInlineWithAltRow &&
      tbl->inlining(cf_id)) {
    auto alt_head = tbl->alt_head(cf_id, row_id);
    (void)alt_head;
  }
  RowCommon<StaticConfig>* newer_rv = head;
  auto rv = head->older_rv;
  // auto head_older = rv;
  // auto latest_wts = rv->wts;

  uint32_t hint_flags = (static_cast<uint32_t>(read_hint) << 1) | static_cast<uint32_t>(write_hint);

  auto_locate(newer_rv, rv, hint_flags);

  if (rv == nullptr) {
    /*
#ifndef NDEBUG
    if (!write_hint) {
      // This usually should not happen; print some debugging information.

      print_version_chain(tbl, row_id);

      assert(ctx_->db_->min_rts() <= ts_);
    }
#endif
    */

    if (StaticConfig::kReserveAfterAbort)
      reserve(tbl, cf_id, row_id, read_hint, write_hint);

    if (StaticConfig::kCollectExtraCommitStats) {
      abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
      abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
    }
    return false;
  }

  rv->rts.update(ts_);
  auto first_rv = rv;

  newer_rv = tbl->head(cf_id, row_id);
  rv = newer_rv->older_rv;

  auto_locate(newer_rv, rv, hint_flags);

  if (rv != first_rv) {
    if (StaticConfig::kReserveAfterAbort)
      reserve(tbl, cf_id, row_id, read_hint, write_hint);

    if (StaticConfig::kCollectExtraCommitStats) {
      abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
      abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
    }
    return false;
  }

  // if (head_older != rv) using_latest_only_ = 0;

  rah.add_fresh_item({access_size_,
                      0,
                      RowAccessState::kPeek,
                      tbl,
                      cf_id,
                      row_id,
                      head,
                      newer_rv,
                      nullptr,
                      rv /*, latest_wts */});

  // Yihe: We don't update access set at all here.
  // We only do it in write_row(), because we don't want to track read set
  // items. For peeks that doesn't involve a explicit read or write, peek_row()
  // with "peek-only" row access handlers should be called (defined below),
  // which doesn't allocate any items in the access set (so should be fine).

  return true;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::peek_row(RAHPO& rah, Table<StaticConfig>* tbl,
                                         uint16_t cf_id, uint64_t row_id,
                                         bool check_dup_access) {
  assert(began_);
  if (rah) return false;

  assert(row_id < tbl->row_count());

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  if (check_dup_access) {
    auto it = access_history_.find({tbl, row_id, cf_id});
    if (it != access_history_.end()) {
      auto idx = it->second;
      assert(idx < access_size_);
      auto item = &accesses_[idx];
      rah.tbl_ = item->tbl;
      rah.cf_id_ = item->cf_id;
      rah.row_id_ = item->row_id;
      if (item->write_rv != nullptr)
        rah.read_rv_ = item->write_rv;
      else
        rah.read_rv_ = item->read_rv;
      return true;
    }
  }

  auto head = tbl->head(cf_id, row_id);
  if (StaticConfig::kInlinedRowVersion && StaticConfig::kInlineWithAltRow &&
      tbl->inlining(cf_id)) {
    auto alt_head = tbl->alt_head(cf_id, row_id);
    (void)alt_head;
  }
  RowCommon<StaticConfig>* newer_rv = head;
  auto rv = head->older_rv;
  // auto head_older = rv;
  // auto latest_wts = rv->wts;

  locate<false, false, false>(newer_rv, rv);

  if (rv == nullptr) return false;

  rah.tbl_ = tbl;
  rah.cf_id_ = cf_id;
  rah.row_id_ = row_id;
  rah.read_rv_ = rv;

  return true;
}

template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::read_row(RAH& rah,
                                         const DataCopier& data_copier) {
  assert(began_);
  if (!rah) return false;

  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  auto item = &rah.item();

  // New rows are readable by default.
  if (item->state == RowAccessState::kNew) return true;

  // OK to read twice.
  if (item->state == RowAccessState::kRead ||
      item->state == RowAccessState::kReadWrite)
    return true;
  if (item->state != RowAccessState::kPeek) return false;

  item->state = RowAccessState::kRead;

  // Yihe: Update rts here early
  //item->read_rv->rts.update(ts_);

  //rset_idx_[rset_size_++] = item->i;

  if (StaticConfig::kInlinedRowVersion &&
      StaticConfig::kPromoteNonInlinedVersion &&
      item->tbl->inlining(item->cf_id)) {
    if (!item->read_rv->is_inlined() &&
        // item->head->older_rv == item->read_rv &&
        item->read_rv->wts < ctx_->db_->min_rts() &&
        item->head->inlined_rv->status == RowVersionStatus::kInvalid) {
      // Promote a version if (1) it is a non-inlined version, (2) the inlined
      // version is not in use, (3) this non-inlined version was created for a
      // while ago.
      return write_row(rah, kDefaultWriteDataSize, data_copier, true);
    }
  }

  return true;
}

template <class StaticConfig>
template <class DataCopier>
bool Transaction<StaticConfig>::write_row(RAH& rah, uint64_t data_size,
                                          const DataCopier& data_copier,
                                          bool check_dup_access) {
  assert(began_);
  if (!rah) return false;

  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  auto item = &rah.item();
  // Yihe: Add to access set (write set) if it's not added yet.
  if (rah.set_item_ == nullptr) {
    if (check_dup_access) {
      auto it = access_history_.find({item->tbl, item->row_id, item->cf_id});
      assert(it == access_history_.end());

      auto ins_res = access_history_.insert({{item->tbl, item->row_id, item->cf_id}, access_size_});
      assert(ins_res.second);
    }

    if (access_size_ >= StaticConfig::kMaxAccessSize) {
      printf("too large access\n");
      assert(false);
    }

    rah.add_item_to_set(accesses_, access_size_);
    access_size_++;
  }

  item = &rah.item();
  assert(item->state != RowAccessState::kPeek);

  // New rows are writable by default.
  if (item->state == RowAccessState::kNew) return true;

  // OK to write twice.
  if (item->state == RowAccessState::kWrite ||
      item->state == RowAccessState::kReadWrite)
    return true;

  if (item->state != RowAccessState::kPeek &&
      item->state != RowAccessState::kRead)
    return false;

  if (data_size == kDefaultWriteDataSize) data_size = item->read_rv->data_size;

  item->write_rv = ctx_->allocate_version_for_existing_row(
      item->tbl, item->cf_id, item->row_id, item->head, data_size);

  if (item->write_rv == nullptr) {
    if (StaticConfig::kCollectExtraCommitStats) {
      abort_reason_target_count_ = &ctx_->stats().aborted_by_get_row_count;
      abort_reason_target_time_ = &ctx_->stats().aborted_by_get_row_time;
    }
    return false;
  }

  item->write_rv->wts = ts_;
  item->write_rv->rts.init(ts_);
  item->write_rv->status = RowVersionStatus::kPending;

  {
    Timing t(ctx_->timing_stack(), &Stats::row_copy);
    if (item->state == RowAccessState::kPeek) {
      if (!data_copier(item->cf_id, item->write_rv, nullptr)) return false;
      item->state = RowAccessState::kWrite;
    } else {
      if (!data_copier(item->cf_id, item->write_rv, item->read_rv))
        return false;
      item->state = RowAccessState::kReadWrite;
    }
  }

  wset_idx_[wset_size_++] = item->i;

  return true;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::delete_row(RAH& rah) {
  assert(began_);
  assert(!peek_only_);

  Timing t(ctx_->timing_stack(), &Stats::execution_write);

  if (!rah) return false;

  auto item = &rah.item();

  switch (item->state) {
    case RowAccessState::kNew:
      item->state = RowAccessState::kInvalid;
      // Immediately deallocate the version (and the row for cf_id 0).
      ctx_->deallocate_version(item->write_rv);
      item->write_rv = nullptr;
      if (item->cf_id == 0) ctx_->deallocate_row(item->tbl, item->row_id);
      break;
    case RowAccessState::kWrite:
      item->state = RowAccessState::kDelete;
      break;
    case RowAccessState::kReadWrite:
      item->state = RowAccessState::kReadDelete;
      break;
    case RowAccessState::kDelete:
    case RowAccessState::kReadDelete:
    // Not OK to delete twice.
    // Fall through.
    default:
      return false;
  }

  rah.reset();

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::auto_locate(RowCommon<StaticConfig>*& newer_rv,
                                            RowVersion<StaticConfig>*& rv,
                                            uint32_t hint_flags) {
  switch (hint_flags) {
    default:
    case 0:
      locate<false, false, false>(newer_rv, rv);
      break;
    case 1:
      locate<false, true, false>(newer_rv, rv);
      break;
    case 2:
      locate<true, false, false>(newer_rv, rv);
      break;
    case 3:
      locate<true, true, false>(newer_rv, rv);
      break;
  }
}

template <class StaticConfig>
template <bool ForRead, bool ForWrite, bool ForValidation>
void Transaction<StaticConfig>::locate(RowCommon<StaticConfig>*& newer_rv,
                                       RowVersion<StaticConfig>*& rv) {
  Timing t(ctx_->timing_stack(), &Stats::execution_read);

  uint64_t chain_len;
  if (StaticConfig::kCollectProcessingStats) chain_len = 0;

  while (true) {
    // This usually should not happen because (1) a new row that can have no new
    // version is not visible unless someone has a dangling row ID (which is
    // rare), and (2) GC ensures that any transaction can find a committed row
    // version whose wts is smaller than that transaction's ts.
    if (rv == nullptr) {
#ifndef NDEBUG
      printf("Transaction:locate(): newer_rv=%p newer_rv->older_rv=%p rv=%p\n",
             newer_rv, newer_rv->older_rv, rv);
#endif
      return;
    }

    if (StaticConfig::kCollectProcessingStats) chain_len++;

    if (rv->wts < ts_) {
      RowVersionStatus status;
      if (StaticConfig::kNoWaitForPending) {
        status = rv->status;
        if ((!StaticConfig::kSkipPending || ForValidation) &&
            status == RowVersionStatus::kPending) {
          rv = nullptr;
          break;
        }
      } else
        status = wait_for_pending(rv);

      if (status == RowVersionStatus::kDeleted) {
        rv = nullptr;
        break;
      } else if (status == RowVersionStatus::kCommitted) {
        break;
      }
      assert((!StaticConfig::kNoWaitForPending &&
              status == RowVersionStatus::kAborted) ||
             StaticConfig::kNoWaitForPending);
    } else
      newer_rv = rv;

    if (StaticConfig::kInsertNewestVersionOnly && ForRead && ForWrite &&
        rv->status != RowVersionStatus::kAborted && rv->wts != ts_) {
      // printf("ts=%" PRIu64 " min_rts %" PRIu64 "\n", ts_.t2,
      //        ctx_->db_->min_rts().t2);
      // printf("rv=%p wts=%" PRIu64 " status=%d\n", rv, rv->wts.t2,
      //        static_cast<int>(rv->status));
      rv = nullptr;
      break;
    }

// if (rv->wts > ts_) newer_rv = rv;
#ifndef NDEBUG
    if (rv->older_rv == nullptr) {
      printf(
          "Transaction:locate(): newer_rv=%p newer_rv->older_rv=%p rv=%p "
          "rv->older_rv=%p\n",
          newer_rv, newer_rv->older_rv, rv, rv->older_rv);
      rv = nullptr;
      return;
    }
#endif
    rv = rv->older_rv;
  }

  if (ForWrite) {
    // Someone have read this row, preventing this row from being overwritten.
    // Thus, abort this transaction.
    if (rv != nullptr && rv->rts.get() > ts_) {
      //printf("Transaction::locate(): write can't overwrite read\n");
      rv = nullptr;
    }
  }

  if (StaticConfig::kCollectProcessingStats) {
    if (ctx_->stats().max_read_chain_len < chain_len)
      ctx_->stats().max_read_chain_len = chain_len;
  }
}

template <class StaticConfig>
RowVersionStatus Transaction<StaticConfig>::wait_for_pending(
    RowVersion<StaticConfig>* rv) {
  if (StaticConfig::kNoWaitForPending) assert(false);

  Timing t(ctx_->timing_stack(), &Stats::wait_for_pending);

  auto status = rv->status;
  while (status == RowVersionStatus::kPending) {
    ::mica::util::pause();
    // usleep(1);
    status = rv->status;
  }
  return status;
}

template <class StaticConfig>
bool Transaction<StaticConfig>::insert_version_deferred() {
  for (auto j = 0; j < wset_size_; j++) {
    auto i = wset_idx_[j];
    auto item = &accesses_[i];
    assert(item->write_rv != nullptr);

    while (true) {
      auto rv = item->newer_rv->older_rv;
      if (item->state == RowAccessState::kReadWrite ||
          item->state == RowAccessState::kReadDelete) {
        locate<true, true, false>(item->newer_rv, rv);
        // Read version changed; abort here without going to validation.
        if (rv != item->read_rv) {
          if (StaticConfig::kReserveAfterAbort)
            reserve(item->tbl, item->cf_id, item->row_id, true, true);
          return false;
        }
      } else {
        assert(item->state == RowAccessState::kWrite ||
               item->state == RowAccessState::kDelete);
        locate<false, true, false>(item->newer_rv, rv);
      }
      if (rv == nullptr) {
        if (StaticConfig::kReserveAfterAbort)
          reserve(item->tbl, item->cf_id, item->row_id, false, true);
        return false;
      }

      auto older_rv = item->newer_rv->older_rv;

      // It seems that newer_rv got a new older_rv node.  We need to find
      // the new value for rv.
      if (older_rv->wts > ts_) continue;

      item->write_rv->older_rv = older_rv;

      // auto actual_older_rv = __sync_val_compare_and_swap(
      //     &item->newer_rv->older_rv, older_rv, item->write_rv);
      //
      // // Found a newly inserted version that could be used as a read version.
      // if (older_rv != actual_older_rv) continue;
      if (!__sync_bool_compare_and_swap(&item->newer_rv->older_rv, older_rv,
                                        item->write_rv))
        continue;

      // Mark the write set item that this row version is visible.
      item->inserted = 1;

      if (rv->rts.get() > ts_) {
        // Oops, someone has updated rts just before the row insert.  We did
        // this checking earlier, but we can do this again to stop inserting
        // more stuff.
        if (StaticConfig::kReserveAfterAbort)
          reserve(item->tbl, item->cf_id, item->row_id,
                  item->state == RowAccessState::kReadWrite ||
                      item->state == RowAccessState::kReadDelete,
                  true);
        return false;
      }
      break;
    }
  }

  return true;
}

template <class StaticConfig>
void Transaction<StaticConfig>::insert_row_deferred() {
  for (auto j = 0; j < iset_size_; j++) {
    auto i = iset_idx_[j];
    auto item = &accesses_[i];

    if (item->state == RowAccessState::kInvalid) continue;

    assert(item->write_rv != nullptr);
    item->head->older_rv = item->write_rv;
    item->write_rv->status = RowVersionStatus::kCommitted;

    item->inserted = 1;
  }
}

template <class StaticConfig>
void Transaction<StaticConfig>::reserve(Table<StaticConfig>* tbl,
                                        uint16_t cf_id, uint64_t row_id,
                                        bool read_hint, bool write_hint) {
  assert(StaticConfig::kReserveAfterAbort);

  to_reserve_.emplace_back(tbl, cf_id, row_id, read_hint, write_hint);
  // to_reserve_.push_back({tbl, row_id, read_hint, write_hint});
}

template <class StaticConfig>
void Transaction<StaticConfig>::print_version_chain(
    const Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id) const {
  auto head = tbl->head(cf_id, row_id);
  auto rv = head->older_rv;

  printf("ts=%" PRIu64 " min_rts %" PRIu64 "\n", ts_.t2,
         ctx_->db_->min_rts().t2);
  while (rv != nullptr) {
    printf("rv=%p wts=%" PRIu64 " status=%d\n", rv, rv->wts.t2,
           static_cast<int>(rv->status));
    rv = rv->older_rv;
  }
  printf("rv=%p\n", rv);
}
}
}

#endif
