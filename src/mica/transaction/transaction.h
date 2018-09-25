#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_H_
#define MICA_TRANSACTION_TRANSACTION_H_

#include "mica/common.h"
#include "mica/transaction/context.h"
#include "mica/transaction/table.h"
#include "mica/transaction/row.h"
#include "mica/transaction/row_access.h"
#include "mica/transaction/timestamp.h"
#include "mica/transaction/stats.h"
#include "mica/util/memcpy.h"

// CS 61 hash function
namespace std {
template <typename T, typename U>
struct hash<pair<T, U>> {
  size_t operator()(const pair<T, U>& x) const {
    size_t h1 = std::hash<T>{}(x.first);
    size_t h2 = std::hash<U>{}(x.second);
    size_t k = 0xC6A4A7935BD1E995UL;
    h2 = ((h2 * k) >> 47) * k;
    return (h1 ^ h2) * k;
  }
};
}

namespace mica {
namespace transaction {
enum class Result {
  kCommitted = 0,
  kAbortedByGetRow,  // Not returned by Transaction::commit() but indicated by
                     // a nullptr return value from get_row_for_write().
  kAbortedByPreValidation,
  kAbortedByDeferredRowVersionInsert,
  kAbortedByMainValidation,
  kAbortedByLogging,
  kInvalid,
};

template <class StaticConfig>
class Transaction {
 public:
  typedef typename StaticConfig::Timing Timing;
  typedef typename StaticConfig::Timestamp Timestamp;
  typedef RowAccessHandle<StaticConfig> RAH;
  typedef RowAccessHandlePeekOnly<StaticConfig> RAHPO;

  static constexpr uint64_t kNewRowID = static_cast<uint64_t>(-1);
  static constexpr uint64_t kDefaultWriteDataSize = static_cast<uint64_t>(-1);

  // transaction_impl/init.h
  Transaction(Context<StaticConfig>* ctx);
  ~Transaction();

  // transaction_impl/commit.h
  bool begin(bool peek_only = false,
             const Timestamp* causally_after_ts = nullptr);

  // transaction_impl/operation.h
  struct NoopDataCopier {
    bool operator()(uint16_t cf_id, RowVersion<StaticConfig>* dest,
                    const RowVersion<StaticConfig>* src) const {
      (void)cf_id;
      (void)dest;
      (void)src;
      return true;
    };
  };
  struct TrivialDataCopier {
    bool operator()(uint16_t cf_id, RowVersion<StaticConfig>* dest,
                    const RowVersion<StaticConfig>* src) const {
      (void)cf_id;
      if (src != nullptr && dest->data_size != 0) {
        assert(dest->data_size >= src->data_size);
        ::mica::util::memcpy(dest->data, src->data, src->data_size);
      }
      return true;
    };
  };

  template <class DataCopier>
  bool new_row(RAH& rah, Table<StaticConfig>* tbl, uint16_t cf_id,
               uint64_t row_id, bool check_dup_access,
               uint64_t data_size, const DataCopier& data_copier);
  void prefetch_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                    uint64_t off, uint64_t len);
  bool peek_row(RAH& rah, Table<StaticConfig>* tbl, uint16_t cf_id,
                uint64_t row_id, bool check_dup_access, bool read_hint,
                bool write_hint);
  bool peek_row(RAHPO& rah, Table<StaticConfig>* tbl, uint16_t cf_id,
                uint64_t row_id, bool check_dup_access);
  template <class DataCopier>
  bool read_row(RAH& rah, const DataCopier& data_copier);
  template <class DataCopier>
  bool write_row(RAH& rah, uint64_t data_size, const DataCopier& data_copier);
  bool delete_row(RAH& rah);

  // transaction_impl/commit.h
  struct NoopWriteFunc {
    bool operator()() const { return true; }
  };
  template <class WriteFunc = NoopWriteFunc>
  bool commit(Result* detail = nullptr,
              const WriteFunc& write_func = WriteFunc());
  bool abort(bool skip_backoff = false);

  bool has_began() const { return began_; }
  bool is_peek_only() const { return peek_only_; }

  Context<StaticConfig>* context() { return ctx_; }
  const Context<StaticConfig>* context() const { return ctx_; }

  const Timestamp& ts() const { return ts_; }

  // For logging an verification.
  uint16_t access_size() const { return access_size_; }
  uint16_t iset_size() const { return iset_size_; }
  uint16_t rset_size() const { return rset_size_; }
  uint16_t wset_size() const { return wset_size_; }
  const uint16_t* iset_idx() const { return iset_idx_; }
  const uint16_t* rset_idx() const { return rset_idx_; }
  const uint16_t* wset_idx() const { return wset_idx_; }
  const RowAccessItem<StaticConfig>* accesses() const { return accesses_; }

  // For debugging.
  void print_version_chain(const Table<StaticConfig>* tbl, uint16_t cf_id,
                           uint64_t row_id) const;

 protected:
  // transaction_impl/operation.h
  void auto_locate(RowCommon<StaticConfig>*& newer_rv,
                   RowVersion<StaticConfig>*& rv, uint32_t hint_flags);
  template <bool ForRead, bool ForWrite, bool ForValidation>
  void locate(RowCommon<StaticConfig>*& newer_rv,
              RowVersion<StaticConfig>*& rv);
  bool insert_version_deferred();
  RowVersionStatus wait_for_pending(RowVersion<StaticConfig>* rv);
  void insert_row_deferred();

  void reserve(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
               bool read_hint, bool write_hint);

  // transaction_impl/commit.h
  Timestamp generate_timestamp();
  void sort_wset();
  bool check_version();
  void update_rts();
  void write();

  void maintenance();
  void backoff();

 private:
  // transaction_impl/commit.h
  Context<StaticConfig>* ctx_;

  bool began_;
  Timestamp ts_;

  uint16_t access_size_;
  uint16_t iset_size_;
  uint16_t rset_size_;
  uint16_t wset_size_;

  uint8_t consecutive_commits_;

  uint8_t peek_only_;

  uint64_t begin_time_;
  uint64_t* abort_reason_target_count_;
  uint64_t* abort_reason_target_time_;

  uint64_t last_commit_time_;

  uint16_t access_bucket_count_;

  RowAccessItem<StaticConfig> accesses_[StaticConfig::kMaxAccessSize];
  uint16_t iset_idx_[StaticConfig::kMaxAccessSize];
  uint16_t rset_idx_[StaticConfig::kMaxAccessSize];
  uint16_t wset_idx_[StaticConfig::kMaxAccessSize];

  struct AccessKey {
    Table<StaticConfig>* tbl;
    uint64_t row_id;
    uint16_t cf_id;

    bool operator==(const AccessKey& o) const {
        return ((tbl == o.tbl) && (row_id == o.row_id)
                               && (cf_id == o.cf_id));
    }
    uint64_t hash() const {
        auto p1 = std::make_pair(tbl, cf_id);
        auto p2 = std::make_pair(p1, row_id);
        return std::hash<decltype(p2)>{}(p2);
    }
  };
  struct AccessKeyHasher {
    std::size_t operator()(const AccessKey& ak) const {
        return ak.hash();
    }
  };
  std::unordered_map<AccessKey, int, AccessKeyHasher> access_history_;

  struct ReserveItem {
    ReserveItem(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                bool read_hint, bool write_hint)
        : tbl(tbl),
          cf_id(cf_id),
          row_id(row_id),
          read_hint(read_hint),
          write_hint(write_hint) {}
    Table<StaticConfig>* tbl;
    uint16_t cf_id;
    uint64_t row_id;
    bool read_hint;
    bool write_hint;
  };
  std::vector<ReserveItem> to_reserve_;
};
}
}

#include "transaction_impl/commit.h"
#include "transaction_impl/init.h"
#include "transaction_impl/operation.h"

#endif
