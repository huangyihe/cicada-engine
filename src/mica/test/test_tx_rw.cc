#include <cstdio>
#include <atomic>
#include <thread>
#include <random>
#include "mica/transaction/db.h"
#include "mica/util/lcore.h"
#include "mica/util/zipf.h"
#include "mica/util/rand.h"
#include "mica/test/test_tx_conf.h"

typedef DBConfig::Alloc Alloc;
typedef DBConfig::Logger Logger;
typedef DBConfig::Timestamp Timestamp;
typedef DBConfig::ConcurrentTimestamp ConcurrentTimestamp;
typedef DBConfig::Timing Timing;
typedef ::mica::transaction::PagePool<DBConfig> PagePool;
typedef ::mica::transaction::DB<DBConfig> DB;
typedef ::mica::transaction::Table<DBConfig> Table;
typedef DB::HashIndexUniqueU64 HashIndex;
typedef DB::BTreeIndexUniqueU64 BTreeIndex;
typedef ::mica::transaction::RowVersion<DBConfig> RowVersion;
typedef ::mica::transaction::RowAccessHandle<DBConfig> RowAccessHandle;
typedef ::mica::transaction::RowAccessHandlePeekOnly<DBConfig>
    RowAccessHandlePeekOnly;
typedef ::mica::transaction::Transaction<DBConfig> Transaction;
typedef ::mica::transaction::Result Result;

static ::mica::util::Stopwatch sw;

// Worker task.

struct Task {
  DB* db;
  Table* tbl;
  HashIndex* hash_idx;
  BTreeIndex* btree_idx;

  uint64_t thread_id;
  uint64_t num_threads;

  // Workload.
  uint64_t num_rows;
  uint64_t tx_count;
  uint64_t num_writes;
  uint64_t num_requests;
  uint64_t row_id_begin;
  uint64_t row_id_end;
  double all_write_ratio;
  double zipf_theta;

  // State (for VerificationLogger).
  uint64_t tx_i;
  uint64_t req_i;
  uint64_t commit_i;

  // Results.
  struct timeval tv_start;
  struct timeval tv_end;

  uint64_t committed;
  uint64_t scanned;

  // Transaction log for verification.
  uint64_t* commit_tx_i;
  Timestamp* commit_ts;
  Timestamp* read_ts;
  Timestamp* write_ts;
} __attribute__((aligned(64)));

template <class StaticConfig>
class VerificationLogger
    : public ::mica::transaction::LoggerInterface<StaticConfig> {
 public:
  VerificationLogger() : tasks(nullptr) {}

  bool log(const ::mica::transaction::Transaction<StaticConfig>* tx) {
    (void)tx;
    return true;
  }

  std::vector<Task>* tasks;
};

template <class Logger>
void setup_logger(Logger* logger, std::vector<Task>* tasks) {
  (void)logger;
  (void)tasks;
}

template <>
void setup_logger(VerificationLogger<DBConfig>* logger,
                  std::vector<Task>* tasks) {
  logger->tasks = tasks;
}

static std::atomic<uint16_t> running_threads;
static std::atomic<uint8_t> stopping;

void worker_proc(Task* task) {
  ::mica::util::lcore.pin_thread(static_cast<uint16_t>(task->thread_id));

  auto ctx = task->db->context();
  auto tbl = task->tbl;
  auto hash_idx = task->hash_idx;
  auto btree_idx = task->btree_idx;

  auto thread_id = task->thread_id;
  auto num_threads = task->num_threads;
  auto num_rows = task->num_rows;
  auto row_id_begin = task->row_id_begin;
  auto row_id_end = task->row_id_end;
  auto thread_num_rows = row_id_end - row_id_begin;
        
  uint64_t seed = 4 * thread_id * ::mica::util::rdtsc();
  uint64_t seed_mask = (uint64_t(1) << 48) - 1;
  ::mica::util::ZipfGen zg(thread_num_rows, task->zipf_theta, seed & seed_mask);
  ::mica::util::Rand u_rand((seed + 1) & seed_mask);

  // ratio of all-write transactions
  uint32_t all_write_threshold = (uint32_t)(task->all_write_ratio * (double)(uint32_t)-1);

  running_threads++;
  while (running_threads.load() < task->num_threads) ::mica::util::pause();

  Timing t(ctx->timing_stack(), &::mica::transaction::Stats::worker);

  gettimeofday(&task->tv_start, nullptr);

  uint64_t next_tx_i = 0;
  uint64_t next_req_i = 0;
  uint64_t commit_i = 0;
  uint64_t scanned = 0;

  task->db->activate(static_cast<uint16_t>(task->thread_id));
  while (task->db->active_thread_count() < task->num_threads) {
    ::mica::util::pause();
    task->db->idle(static_cast<uint16_t>(task->thread_id));
  }

  if (kVerbose) printf("lcore %" PRIu64 "\n", task->thread_id);

  Transaction tx(ctx);

  while (next_tx_i < task->tx_count && (stopping.load() == 0)) {
    uint64_t tx_i;
    uint64_t req_i;

    bool all_writes = u_rand.next_u32() < all_write_threshold;
    uint64_t req_count = all_writes ? task->num_writes : task->num_requests;

    tx_i = next_tx_i++;
    req_i = next_req_i;
    next_req_i += req_count;

    task->tx_i = tx_i;
    task->req_i = req_i;
    task->commit_i = commit_i;

    while (true) {
      bool aborted = false;

      uint64_t v = 0;

      // Yihe TODO: add in support for generating read-only transactions
      bool use_peek_only = false; //kUseSnapshot && task->read_only_tx[tx_i];

      bool ret = tx.begin(use_peek_only);
      assert(ret);
      (void)ret;

      for (uint64_t req_j = 0; req_j < req_count; req_j++) {
        bool is_read = !all_writes && (req_j != (2 * req_count / 3));
        bool is_rmw = !is_read;
        uint64_t row_id = all_writes ? zg.next() + row_id_begin :
          (((is_read) ? (u_rand.next_u32() % num_threads * thread_num_rows + zg.next()) :
            u_rand.next_u32()) % num_rows);
        uint64_t virtual_row_id = row_id;
        uint8_t column_id = static_cast<uint8_t>(u_rand.next_u32() %
                                                 (kDataSize / kColumnSize));

        if (hash_idx != nullptr) {
          auto lookup_result =
              hash_idx->lookup(&tx, row_id, kSkipValidationForIndexAccess,
                               [&row_id](auto& k, auto& v) {
                                 (void)k;
                                 row_id = v;
                                 return false;
                               });
          if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
            assert(false);
            tx.abort();
            aborted = true;
            break;
          }
        } else if (btree_idx != nullptr) {
          auto lookup_result =
              btree_idx->lookup(&tx, row_id, kSkipValidationForIndexAccess,
                                [&row_id](auto& k, auto& v) {
                                  (void)k;
                                  row_id = v;
                                  return false;
                                });
          if (lookup_result != 1 || lookup_result == BTreeIndex::kHaveToAbort) {
            assert(false);
            tx.abort();
            aborted = true;
            break;
          }
        }

        if (!use_peek_only) {
          RowAccessHandle rah(&tx);

          if (is_read) {
            if (!rah.peek_row(tbl, 0, row_id, false, true, false) ||
                !rah.read_row()) {
              tx.abort();
              aborted = true;
              break;
            }

            const char* data =
                rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64)
              v += static_cast<uint64_t>(data[j]);
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
          } else {
            if (is_rmw) {
              if (!rah.peek_row(tbl, 0, row_id, false, true, true) ||
                  !rah.read_row() || !rah.write_row(kDataSize)) {
                tx.abort();
                aborted = true;
                break;
              }
            } else {
              if (!rah.peek_row(tbl, 0, row_id, false, false, true) ||
                  !rah.write_row(kDataSize)) {
                tx.abort();
                aborted = true;
                break;
              }
            }

            char* data =
                rah.data() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64) {
              v += static_cast<uint64_t>(data[j]);
              data[j] = static_cast<char>(v);
            }
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
            data[kColumnSize - 1] = static_cast<char>(v);
          }
        } else {
          if (!kUseScan) {
            RowAccessHandlePeekOnly rah(&tx);

            if (!rah.peek_row(tbl, 0, row_id, false, false, false)) {
              tx.abort();
              aborted = true;
              break;
            }

            const char* data =
                rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
            for (uint64_t j = 0; j < kColumnSize; j += 64)
              v += static_cast<uint64_t>(data[j]);
            v += static_cast<uint64_t>(data[kColumnSize - 1]);
          } else if (!kUseFullTableScan) {
            RowAccessHandlePeekOnly rah(&tx);

            uint64_t next_row_id = row_id;
            uint64_t next_next_raw_row_id = virtual_row_id + 1;
            if (next_next_raw_row_id == task->num_rows)
              next_next_raw_row_id = 0;

            uint32_t scan_len = static_cast<uint32_t>(req_count);
            for (uint32_t scan_i = 0; scan_i < scan_len; scan_i++) {
              uint64_t this_row_id = next_row_id;

              // TODO: Support btree_idx.
              assert(hash_idx != nullptr);

              // Lookup index for next row.
              auto lookup_result =
                  hash_idx->lookup(&tx, next_next_raw_row_id, true,
                                   [&next_row_id](auto& k, auto& v) {
                                     (void)k;
                                     next_row_id = v;
                                     return false;
                                   });
              if (lookup_result != 1 ||
                  lookup_result == HashIndex::kHaveToAbort) {
                tx.abort();
                aborted = true;
                break;
              }

              // Prefetch index for next next row.
              next_next_raw_row_id++;
              if (next_next_raw_row_id == task->num_rows)
                next_next_raw_row_id = 0;
              hash_idx->prefetch(&tx, next_next_raw_row_id);

              // Prefetch next row.
              rah.prefetch_row(tbl, 0, next_row_id,
                               static_cast<uint64_t>(column_id) * kColumnSize,
                               kColumnSize);

              // Access current row.
              if (!rah.peek_row(tbl, 0, this_row_id, false, false, false)) {
                tx.abort();
                aborted = true;
                break;
              }

              const char* data =
                  rah.cdata() + static_cast<uint64_t>(column_id) * kColumnSize;
              for (uint64_t j = 0; j < kColumnSize; j += 64)
                v += static_cast<uint64_t>(data[j]);
              v += static_cast<uint64_t>(data[kColumnSize - 1]);

              rah.reset();
            }

            if (aborted) break;
          } else /*if (kUseFullTableScan)*/ {
            if (!tbl->scan(&tx, 0,
                           static_cast<uint64_t>(column_id) * kColumnSize,
                           kColumnSize, [&v, column_id](auto& rah) {
                             const char* data =
                                 rah.cdata() +
                                 static_cast<uint64_t>(column_id) * kColumnSize;
                             for (uint64_t j = 0; j < kColumnSize; j += 64)
                               v += static_cast<uint64_t>(data[j]);
                             v += static_cast<uint64_t>(data[kColumnSize - 1]);
                           })) {
              tx.abort();
              aborted = true;
              break;
            }
          }
        }
      }

      if (aborted) continue;

      Result result;
      if (!tx.commit(&result)) continue;
      assert(result == Result::kCommitted);

      commit_i++;
      if (kUseScan && use_peek_only) {
        if (!kUseFullTableScan)
          scanned += req_count;
        else
          scanned += task->num_rows;
      }

      break;
    }
  }

  task->db->deactivate(static_cast<uint16_t>(task->thread_id));

  if (stopping.load() == 0) {
    stopping.store(1);
  }

  task->committed = commit_i;
  task->scanned = scanned;

  gettimeofday(&task->tv_end, nullptr);
}

int main(int argc, const char* argv[]) {
  if (argc != 8) {
    printf(
        "%s NUM-ROWS REQS-PER-TX REQS-PER-WR-TX WR-TX-RATIO ZIPF-THETA TX-COUNT "
        "THREAD-COUNT\n",
        argv[0]);
    return EXIT_FAILURE;
  }

  auto config = ::mica::util::Config::load_file("test_tx.json");

  uint64_t num_rows = static_cast<uint64_t>(atol(argv[1]));
  uint64_t reqs_per_tx = static_cast<uint64_t>(atol(argv[2]));
  uint64_t reqs_per_wr_tx = static_cast<uint64_t>(atol(argv[3]));
  double all_write_ratio = atof(argv[4]);
  double zipf_theta = atof(argv[5]);
  uint64_t tx_count = static_cast<uint64_t>(atol(argv[6]));
  uint64_t num_threads = static_cast<uint64_t>(atol(argv[7]));

  Alloc alloc(config.get("alloc"));
  auto page_pool_size = 24 * uint64_t(1073741824);
  PagePool* page_pools[2];
  // if (num_threads == 1) {
  //   page_pools[0] = new PagePool(&alloc, page_pool_size, 0);
  //   page_pools[1] = nullptr;
  // } else {
  page_pools[0] = new PagePool(&alloc, page_pool_size / 2, 0);
  page_pools[1] = new PagePool(&alloc, page_pool_size / 2, 1);
  // }

  ::mica::util::lcore.pin_thread(0);

  sw.init_start();
  sw.init_end();

  if (num_rows == 0) {
    num_rows = SYNTH_TABLE_SIZE;
    reqs_per_tx = REQ_PER_QUERY;
    reqs_per_wr_tx = 3;
    all_write_ratio = 0.1;
    zipf_theta = ZIPF_THETA;
    tx_count = MAX_TXN_PER_PART;
    num_threads = THREAD_CNT;
#ifndef NDEBUG
    printf("!NDEBUG\n");
    return EXIT_FAILURE;
#endif
  }

  printf("num_rows = %" PRIu64 "\n", num_rows);
  printf("reqs_per_tx = %" PRIu64 "\n", reqs_per_tx);
  printf("reqs_per_wr_tx = %" PRIu64 "\n", reqs_per_wr_tx);
  printf("all_write_ratio = %lf\n", all_write_ratio);
  printf("zipf_theta = %lf\n", zipf_theta);
  printf("tx_count = %" PRIu64 "\n", tx_count);
  printf("num_threads = %" PRIu64 "\n", num_threads);
#ifndef NDEBUG
  printf("!NDEBUG\n");
#endif
  printf("\n");

  Logger logger;
  DB db(page_pools, &logger, &sw, static_cast<uint16_t>(num_threads));

  const bool kVerify =
      typeid(typename DBConfig::Logger) == typeid(VerificationLogger<DBConfig>);

  const uint64_t data_sizes[] = {kDataSize};
  bool ret = db.create_table("main", 1, data_sizes);
  assert(ret);
  (void)ret;

  auto tbl = db.get_table("main");

  db.activate(0);

  HashIndex* hash_idx = nullptr;
  if (kUseHashIndex) {
    bool ret = db.create_hash_index_unique_u64("main_idx", tbl, num_rows);
    assert(ret);
    (void)ret;

    hash_idx = db.get_hash_index_unique_u64("main_idx");
    Transaction tx(db.context(0));
    hash_idx->init(&tx);
  }

  BTreeIndex* btree_idx = nullptr;
  if (kUseBTreeIndex) {
    bool ret = db.create_btree_index_unique_u64("main_idx", tbl);
    assert(ret);
    (void)ret;

    btree_idx = db.get_btree_index_unique_u64("main_idx");
    Transaction tx(db.context(0));
    btree_idx->init(&tx);
  }

  {
    printf("initializing table\n");

    std::vector<std::thread> threads;
    uint64_t init_num_threads = std::min(uint64_t(2), num_threads);
    for (uint64_t thread_id = 0; thread_id < init_num_threads; thread_id++) {
      threads.emplace_back([&, thread_id] {
        ::mica::util::lcore.pin_thread(thread_id);

        db.activate(static_cast<uint16_t>(thread_id));
        while (db.active_thread_count() < init_num_threads) {
          ::mica::util::pause();
          db.idle(static_cast<uint16_t>(thread_id));
        }

        // Randomize the data layout by shuffling row insert order.
        std::mt19937 g(thread_id);
        std::vector<uint64_t> row_ids;
        row_ids.reserve((num_rows + init_num_threads - 1) / init_num_threads);
        for (uint64_t i = thread_id; i < num_rows; i += init_num_threads)
          row_ids.push_back(i);
        std::shuffle(row_ids.begin(), row_ids.end(), g);

        Transaction tx(db.context(static_cast<uint16_t>(thread_id)));
        const uint64_t kBatchSize = 16;
        for (uint64_t i = 0; i < row_ids.size(); i += kBatchSize) {
          while (true) {
            bool ret = tx.begin();
            if (!ret) {
              printf("failed to start a transaction\n");
              continue;
            }

            bool aborted = false;
            auto i_end = std::min(i + kBatchSize, row_ids.size());
            for (uint64_t j = i; j < i_end; j++) {
              RowAccessHandle rah(&tx);
              if (!rah.new_row(tbl, 0, Transaction::kNewRowID, true,
                               kDataSize)) {
                // printf("failed to insert rows at new_row(), row = %" PRIu64
                //        "\n",
                //        j);
                aborted = true;
                tx.abort();
                break;
              }

              if (kUseHashIndex) {
                auto ret = hash_idx->insert(&tx, row_ids[j], rah.row_id());
                if (ret != 1 || ret == HashIndex::kHaveToAbort) {
                  // printf("failed to update index row = %" PRIu64 "\n", j);
                  aborted = true;
                  tx.abort();
                  break;
                }
              }
              if (kUseBTreeIndex) {
                auto ret = btree_idx->insert(&tx, row_ids[j], rah.row_id());
                if (ret != 1 || ret == BTreeIndex::kHaveToAbort) {
                  // printf("failed to update index row = %" PRIu64 "\n", j);
                  aborted = true;
                  tx.abort();
                  break;
                }
              }
            }

            if (aborted) continue;

            Result result;
            if (!tx.commit(&result)) {
              // printf("failed to insert rows at commit(), row = %" PRIu64
              //        "; result=%d\n",
              //        i_end - 1, static_cast<int>(result));
              continue;
            }
            break;
          }
        }

        db.deactivate(static_cast<uint16_t>(thread_id));
        return 0;
      });
    }

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }

    // TODO: Use multiple threads to renew rows for more balanced memory access.

    db.activate(0);
    {
      uint64_t i = 0;
      tbl->renew_rows(db.context(0), 0, i, static_cast<uint64_t>(-1), false);
    }
    if (hash_idx != nullptr) {
      uint64_t i = 0;
      hash_idx->index_table()->renew_rows(db.context(0), 0, i,
                                          static_cast<uint64_t>(-1), false);
    }
    if (btree_idx != nullptr) {
      uint64_t i = 0;
      btree_idx->index_table()->renew_rows(db.context(0), 0, i,
                                           static_cast<uint64_t>(-1), false);
    }
    db.deactivate(0);

    db.reset_stats();
    db.reset_backoff();
  }

  std::vector<Task> tasks(num_threads);
  setup_logger(&logger, &tasks);
  {
    printf("generating workload\n");

    if (kUseContendedSet) zipf_theta = 0.;

    uint64_t num_rows_div = num_rows / num_threads;
    for (uint64_t thread_id = 0; thread_id < num_threads; thread_id++) {
      tasks[thread_id].thread_id = static_cast<uint16_t>(thread_id);
      tasks[thread_id].num_threads = num_threads;
      tasks[thread_id].db = &db;
      tasks[thread_id].tbl = tbl;
      tasks[thread_id].hash_idx = hash_idx;
      tasks[thread_id].btree_idx = btree_idx;
      tasks[thread_id].num_rows = num_rows;
      tasks[thread_id].tx_count = tx_count;
      tasks[thread_id].num_writes = reqs_per_wr_tx;
      tasks[thread_id].num_requests = reqs_per_tx;
      tasks[thread_id].row_id_begin = thread_id * num_rows_div;
      tasks[thread_id].row_id_end = (thread_id == num_threads - 1) ? num_rows : ((thread_id + 1) * num_rows_div);
      tasks[thread_id].all_write_ratio = all_write_ratio;
      tasks[thread_id].zipf_theta = zipf_theta;
    }
  }

  // tbl->print_table_status();
  //
  // if (kShowPoolStats) db.print_pool_status();

  // For verification.
  std::vector<Timestamp> table_ts;

  for (auto phase = 0; phase < 2; phase++) {
    // if (kVerify && phase == 0) {
    //   printf("skipping warming up\n");
    //   continue;
    // }

    if (kVerify && phase == 1) {
      for (uint64_t row_id = 0; row_id < num_rows; row_id++) {
        auto rv = tbl->latest_rv(0, row_id);
        table_ts.push_back(rv->wts);
      }
    }

    if (phase == 0)
      printf("warming up\n");
    else {
      db.reset_stats();
      printf("executing workload\n");
    }

    running_threads.store(0);
    stopping.store(0);

    ::mica::util::memory_barrier();

    std::vector<std::thread> threads;
    for (uint64_t thread_id = 1; thread_id < num_threads; thread_id++)
      threads.emplace_back(worker_proc, &tasks[thread_id]);

    if (phase != 0 && kRunPerf) {
      int r = system("perf record -a sleep 1 &");
      // int r = system("perf record -a -g sleep 1 &");
      (void)r;
    }

    worker_proc(&tasks[0]);

    while (threads.size() > 0) {
      threads.back().join();
      threads.pop_back();
    }
  }
  printf("\n");

  {
    double diff;
    {
      double min_start = 0.;
      double max_end = 0.;
      for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
        double start = (double)tasks[thread_id].tv_start.tv_sec * 1. +
                       (double)tasks[thread_id].tv_start.tv_usec * 0.000001;
        double end = (double)tasks[thread_id].tv_end.tv_sec * 1. +
                     (double)tasks[thread_id].tv_end.tv_usec * 0.000001;
        if (thread_id == 0 || min_start > start) min_start = start;
        if (thread_id == 0 || max_end < end) max_end = end;
      }

      diff = max_end - min_start;
    }
    double total_time = diff * static_cast<double>(num_threads);

    uint64_t total_committed = 0;
    uint64_t total_scanned = 0;
    for (size_t thread_id = 0; thread_id < num_threads; thread_id++) {
      total_committed += tasks[thread_id].committed;
      if (kUseScan) total_scanned += tasks[thread_id].scanned;
    }
    printf("throughput:                   %7.3lf M/sec\n",
           static_cast<double>(total_committed) / diff * 0.000001);
    if (kUseScan)
      printf("scan throughput:              %7.3lf M/sec\n",
             static_cast<double>(total_scanned) / diff * 0.000001);

    db.print_stats(diff, total_time);

    tbl->print_table_status();

    if (hash_idx != nullptr) hash_idx->index_table()->print_table_status();
    if (btree_idx != nullptr) btree_idx->index_table()->print_table_status();

    if (kShowPoolStats) db.print_pool_status();
  }

  {
    printf("cleaning up\n");
  }

  return EXIT_SUCCESS;
}
