#pragma once
#ifndef MICA_TRANSACTION_PAGE_POOL_H_
#define MICA_TRANSACTION_PAGE_POOL_H_

#include <cstdio>
#include <stdexcept>
#include "mica/util/lcore.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class PagePool {
 public:
  typedef typename StaticConfig::Alloc Alloc;

  static constexpr uint64_t kPageSize = 2 * 1048576;

  PagePool(Alloc* alloc, uint64_t size, uint8_t numa_id)
      : alloc_(alloc), numa_id_(numa_id) {
    if (numa_id >= ::mica::util::lcore.numa_count()) {
      throw std::runtime_error("Invalid numa_id.");
    }
    uint64_t page_count = (size + kPageSize - 1) / kPageSize;
    size_ = page_count * kPageSize;

    lock_ = 0;
    total_count_ = page_count;
    free_count_ = page_count;

    size_t lowest_lcore_in_numa_node = 0;
    while (::mica::util::lcore.numa_id(lowest_lcore_in_numa_node) !=
             size_t(numa_id)) {
      ++lowest_lcore_in_numa_node;
      if (lowest_lcore_in_numa_node >= ::mica::util::lcore.lcore_count()) {
        throw std::runtime_error("mica::util::lcore internal error.");
      }
    }

    pages_ =
        reinterpret_cast<char*>(
          alloc_->malloc_contiguous(size_, lowest_lcore_in_numa_node));
    if (!pages_) {
      printf("failed to initialize PagePool\n");
      throw std::runtime_error("Failed to construct PagePool.");
      return;
    }
    for (uint64_t i = 0; i < page_count; i++)
      *reinterpret_cast<char**>(pages_ + i * kPageSize) =
          pages_ + (i + 1) * kPageSize;

    *reinterpret_cast<char**>(pages_ + (page_count - 1) * kPageSize) = nullptr;
    next_ = pages_;

    printf("initialized PagePool on numa node %" PRIu8 " with %.3lf GB\n",
           numa_id_, static_cast<double>(size) / 1000000000.);
  }

  ~PagePool() { alloc_->free_striped(pages_); }

  char* allocate() {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    auto p = next_;
    if (next_) {
      next_ = *reinterpret_cast<char**>(next_);
      free_count_--;
    }

    __sync_lock_release(&lock_);

    return p;
  }

  void free(char* p) {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    *reinterpret_cast<char**>(p) = next_;
    next_ = p;
    free_count_++;

    __sync_lock_release(&lock_);
  }

  uint8_t numa_id() const { return numa_id_; }

  uint64_t total_count() const { return total_count_; }
  uint64_t free_count() const { return free_count_; }

  void print_status() const {
    printf("PagePool on numa node %" PRIu8 "\n", numa_id_);
    printf("  in use: %7.3lf GB\n",
           static_cast<double>((total_count_ - free_count_) * kPageSize) /
               1000000000.);
    printf("  free:   %7.3lf GB\n",
           static_cast<double>(free_count_ * kPageSize) / 1000000000.);
    printf("  total:  %7.3lf GB\n",
           static_cast<double>(total_count_ * kPageSize) / 1000000000.);
  }

 private:
  Alloc* alloc_;
  uint64_t size_;
  uint8_t numa_id_;

  uint64_t total_count_;
  char* pages_;

  volatile uint32_t lock_;
  uint64_t free_count_;
  char* next_;
} __attribute__((aligned(64)));
}
}

#endif
