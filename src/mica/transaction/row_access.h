#pragma once
#ifndef MICA_TRANSACTION_TRANSACTION_ROW_ACCESS_H_
#define MICA_TRANSACTION_TRANSACTION_ROW_ACCESS_H_

#include "mica/common.h"
#include "mica/transaction/table.h"
#include "mica/transaction/row.h"
#include "mica/transaction/stats.h"
#include "mica/transaction/row_version_pool.h"

namespace mica {
namespace transaction {
// State chart:
//
// Initial accesses:
// new():             . -> kNew
// peek():            . -> kPeek
//
// Upgrades:
// read():        kPeek -> kRead
// write():       kPeek -> kWrite
// write():       kRead -> kReadWrite
// delete():     kWrite -> kDelete
// delete(): kReadWrite -> kReadDelete
// delete():       kNew -> .

enum class RowAccessState : uint8_t {
  kInvalid = 0,

  kNew,         // Has write_rv
  kPeek,        // Has read_rv
                //
  kRead,        // Has read_rv
  kReadWrite,   // Has write_rv, read_rv
  kWrite,       // Has write_rv, read_rv
  kDelete,      // Has write_rv, read_rv
  kReadDelete,  // Has write_rv, read_rv
};

template <class StaticConfig>
class Transaction;

template <class StaticConfig>
struct RowAccessItem {
  // Invariant: newer_rv.wts > (write_rv.wts) > read_rv.wts.

  uint16_t i;
  uint8_t inserted;
  RowAccessState state;

  Table<StaticConfig>* tbl;
  uint16_t cf_id;
  uint64_t row_id;

  RowHead<StaticConfig>* head;
  RowCommon<StaticConfig>* newer_rv;
  RowVersion<StaticConfig>* write_rv;
  RowVersion<StaticConfig>* read_rv;

  // typename StaticConfig::Timestamp latest_wts;

  RowAccessItem& operator=(const RowAccessItem& o) = default;
};

template <class StaticConfig>
class RowAccessHandle {
 public:
  Transaction<StaticConfig>* tx() { return tx_; }
  const Transaction<StaticConfig>* tx() const { return tx_; }

  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool new_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
               bool check_dup_access, uint64_t data_size,
               const DataCopier& data_copier = DataCopier()) {
    return tx_->new_row(*this, tbl, cf_id, row_id, check_dup_access, data_size,
                        data_copier);
  }
  void prefetch_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                    uint64_t off, uint64_t len) {
    tx_->prefetch_row(tbl, cf_id, row_id, off, len);
  }
  bool peek_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                bool check_dup_access, bool read_hint, bool write_hint) {
    return tx_->peek_row(*this, tbl, cf_id, row_id, check_dup_access, read_hint,
                         write_hint);
  }
  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool read_row(const DataCopier& data_copier = DataCopier()) {
    return tx_->read_row(*this, data_copier);
  }
  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool write_row(
      uint64_t data_size = Transaction<StaticConfig>::kDefaultWriteDataSize,
      const DataCopier& data_copier = DataCopier(), bool check_dup_access = true) {
    return tx_->write_row(*this, data_size, data_copier, check_dup_access);
  }
  bool delete_row() { return tx_->delete_row(*this); }

  RowAccessState state() const {
    if (*this)
      return item().state;
    else
      return RowAccessState::kInvalid;
  }
  operator bool() const { return valid_; }
  bool operator!() const { return !valid_; }

  Table<StaticConfig>* table() { return item().tbl; }
  const Table<StaticConfig>* table() const { return item().tbl; }

  uint16_t cf_id() const { return item().cf_id; }

  uint64_t row_id() const { return item().row_id; }

  bool can_read() const {
    return item().write_rv != nullptr ||
           item().read_rv != nullptr;
  }
  bool can_write() const { return item().write_rv != nullptr; }
  bool is_deleted() const {
    return (!valid_) || (item().write_rv != nullptr &&
                                       item().write_rv->deleted) ||
           (item().read_rv != nullptr && item().read_rv->deleted);
  }

  const char* cdata() const {
    if (item().write_rv != nullptr)
      return item().write_rv->data;
    else if (item().read_rv != nullptr)
      return item().read_rv->data;
    else
      return nullptr;
  }

  char* data() {
    if (item().write_rv != nullptr)
      return item().write_rv->data;
    else
      return nullptr;
  }

  uint64_t size() const { return item().tbl->data_size(); }

  uint64_t rv_size() const {
    if (item().write_rv != nullptr)
      return SharedRowVersionPool<StaticConfig>::class_to_size(
          item().write_rv->size_cls);
    else if (item().read_rv != nullptr)
      return SharedRowVersionPool<StaticConfig>::class_to_size(
          item().read_rv->size_cls);
    else
      return 0;
  }

  void reset() { valid_ = false; set_item_ = nullptr; }

  void add_fresh_item(RowAccessItem<StaticConfig>&& item) {
    assert(!valid_ && (set_item_ == nullptr));
    valid_ = true;
    local_item_ = item;
  }

  void add_set_item(RowAccessItem<StaticConfig>* const tx_accesses, int index) {
    assert(!valid_ && (set_item_ == nullptr));
    valid_ = true;
    set_item_ = &tx_accesses[index];
    assert(index <= static_cast<int>(std::numeric_limits<uint16_t>::max()));
    assert(set_item_->i == static_cast<uint16_t>(index));
  }

  void add_item_to_set(RowAccessItem<StaticConfig>* const tx_accesses, int index) {
    assert(valid_ && (set_item_ == nullptr));
    auto dst = &tx_accesses[index];
    // Don't forget to set the actual index!!
    assert(index <= static_cast<int>(std::numeric_limits<uint16_t>::max()));
    local_item_.i = static_cast<uint16_t>(index);
    *dst = local_item_;
    set_item_ = dst;
  }

  const RowAccessItem<StaticConfig>& item() const {
    assert(valid_);
    if (set_item_ != nullptr) {
      return *set_item_;
    } else {
      return local_item_;
    }
  }

  RowAccessItem<StaticConfig>& item() {
    assert(valid_);
    if (set_item_ != nullptr) {
      return *set_item_;
    } else {
      return local_item_;
    }
  }

  RowAccessHandle()
      : tx_(nullptr), valid_(false),
        set_item_(nullptr), local_item_() {}

  explicit RowAccessHandle(Transaction<StaticConfig>* tx)
      : tx_(tx), valid_(false),
        set_item_(nullptr), local_item_() {}

  //RowAccessHandle(const RowAccessHandle& o)
  //    : tx_(o.tx_), valid_(o.valid_),
  //      set_item_(set_item_), local_item_() {
  //    if (valid_ && (set_item_ == nullptr)) {
  //      local_item_ = o.local_item_;
  //    }
  //}

  RowAccessHandle& operator=(const RowAccessHandle& o) {
    tx_ = o.tx_;
    valid_ = o.valid_;
    set_item_ = o.set_item_;
    if (valid_ && (set_item_ == nullptr)) {
      local_item_ = o.local_item_;
    }
    return *this;
  }

 private:
  friend Transaction<StaticConfig>;

  Transaction<StaticConfig>* tx_;
  bool valid_;
  RowAccessItem<StaticConfig>* set_item_;
  RowAccessItem<StaticConfig> local_item_;
};

template <class StaticConfig>
class RowAccessHandlePeekOnly {
 public:
  Transaction<StaticConfig>* tx() { return tx_; }
  const Transaction<StaticConfig>* tx() const { return tx_; }

  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool new_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
               bool check_dup_access, uint64_t data_size,
               const DataCopier& data_copier = DataCopier()) {
    (void)tbl;
    (void)cf_id;
    (void)row_id;
    (void)check_dup_access;
    (void)data_size;
    (void)data_copier;
    return false;
  }
  void prefetch_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                    uint64_t off, uint64_t len) {
    tx_->prefetch_row(tbl, cf_id, row_id, off, len);
  }
  bool peek_row(Table<StaticConfig>* tbl, uint16_t cf_id, uint64_t row_id,
                bool check_dup_access, bool read_hint, bool write_hint) {
    (void)read_hint;
    (void)write_hint;
    return tx_->peek_row(*this, tbl, cf_id, row_id, check_dup_access);
  }
  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool read_row(const DataCopier& data_copier = DataCopier()) {
    (void)data_copier;
    return false;
  }
  template <
      class DataCopier = typename Transaction<StaticConfig>::TrivialDataCopier>
  bool write_row(
      uint64_t data_size = Transaction<StaticConfig>::kDefaultWriteDataSize,
      const DataCopier& data_copier = DataCopier(), bool check_dup_access = true) {
    (void)data_size;
    (void)data_copier;
    (void)check_dup_access;
    return false;
  }
  bool delete_row() { return false; }

  RowAccessState state() const {
    if (*this)
      return RowAccessState::kPeek;
    else
      return RowAccessState::kInvalid;
  }
  operator bool() const { return read_rv_ != nullptr; }
  bool operator!() const { return read_rv_ == nullptr; }

  Table<StaticConfig>* table() { return tbl_; }
  const Table<StaticConfig>* table() const { return tbl_; }

  uint16_t cf_id() const { return cf_id_; }

  uint64_t row_id() const { return row_id_; }

  bool can_read() const { return read_rv_ != nullptr; }
  bool can_write() const { return false; }
  bool is_deleted() const { return read_rv_ != nullptr && read_rv_->deleted; }

  const char* cdata() const {
    if (read_rv_ != nullptr)
      return read_rv_->data;
    else
      return nullptr;
  }

  char* data() { return nullptr; }

  size_t size() const { return tbl_->data_size(); }

  void reset() { read_rv_ = nullptr; }

  RowAccessHandlePeekOnly() : read_rv_(nullptr) {}

  explicit RowAccessHandlePeekOnly(Transaction<StaticConfig>* tx)
      : tx_(tx), tbl_(nullptr), cf_id_(0), row_id_(0), read_rv_(nullptr) {}

  RowAccessHandlePeekOnly(const RowAccessHandlePeekOnly& o)
      : tx_(o.tx_),
        tbl_(o.tbl_),
        cf_id_(o.cf_id_),
        row_id_(o.row_id_),
        read_rv_(nullptr) {}

  RowAccessHandlePeekOnly& operator=(const RowAccessHandlePeekOnly& o) {
    tx_ = o.tx_;
    tbl_ = o.tbl_;
    cf_id_ = o.cf_id_;
    row_id_ = o.row_id_;
    read_rv_ = nullptr;
    return *this;
  }

 private:
  friend Transaction<StaticConfig>;

  Transaction<StaticConfig>* tx_;

  Table<StaticConfig>* tbl_;
  uint16_t cf_id_;
  uint64_t row_id_;
  RowVersion<StaticConfig>* read_rv_;
};

}
}

#endif
