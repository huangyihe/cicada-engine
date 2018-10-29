#pragma once

#include <inttypes.h>
#include <sstream>
#include <string>
#include <map>
#include <unordered_map>

namespace mica {
namespace transaction {

// struct AccessKey (transaction.h) is used as key
template <class KeyType, class KeyHasher>
class HHAccounter {
public:
  // irm -> item reverse map
  typedef std::unordered_map<KeyType, std::string, KeyHasher> irm_type;

  HHAccounter(size_t num_cnts, irm_type* map)
    : n_(num_cnts), cnts_(new CntElem[num_cnts]()), item_reverse_map(map) {}

  ~HHAccounter() {
    delete[] cnts_;
  }

  static irm_type* new_irm() {
    return new irm_type();
  }

  std::string irm_lookup(const KeyType& key) const {
    auto it = item_reverse_map->find(key);
    if (it != item_reverse_map->end()) {
      return it->second;
    } else {
      return static_cast<std::string>(key);
    }
  }

  void irm_add(const KeyType& key, const std::string&& pretty_name) {
    item_reverse_map->insert({ key, pretty_name });
  }

  void account(const KeyType&& key) {
    size_t empty_slot = n_;
    for (size_t i = 0; i < n_; ++i) {
      auto& c = cnts_[i];
      if (c.k_ == key) {
        ++c.cnt_;
        return;
      }
      if (empty_slot == n_ && c.cnt_ == 0) {
        empty_slot = i;
      }
    }

    if (empty_slot != n_) {
      cnts_[empty_slot].k_ = key;
      ++cnts_[empty_slot].cnt_;
      return;
    }

    for (size_t i = 0; i < n_; ++i) {
      if (cnts_[i].cnt_ != 0)
        --cnts_[i].cnt_;
    }
  }

  std::string dump_stats() const {
    std::stringstream ss;
    std::map<uint64_t, size_t> sort;
    for (size_t i = 0; i < n_; ++i) {
      auto& c = cnts_[i];
      if (c.cnt_ != 0)
        sort.insert({ c.cnt_, i });
    }
    for (auto it = sort.rbegin(); it != sort.rend(); ++it) {
      auto& c = cnts_[it->second];
      ss << "Access item: " << irm_lookup(c.k_);
      ss << ", count=" << it->first << std::endl;
    }
    return ss.str();
  }

private:
  struct CntElem {
    KeyType k_;
    uint64_t cnt_;

    CntElem() : k_(), cnt_(0) {}
  };

  size_t n_;
  CntElem *cnts_;
  irm_type *item_reverse_map;
};

}
}
