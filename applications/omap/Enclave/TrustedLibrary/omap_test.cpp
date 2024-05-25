#include "../Enclave.h"
#include "Enclave_t.h"
#include "oram/pathoram/oram.hpp"
#include "otree/otree.hpp"
////
using ETH_Addr = _OBST::K;

using ERC20_Balance = _OBST::V;
#define MB << 20
#define ASSERT_TRUE(expr)                           \
  if (!expr) {                                      \
    printf("assert failed at line %d\n", __LINE__); \
    abort();                                        \
  }

// using namespace ODSL;
EM::Backend::MemServerBackend* EM::Backend::g_DefaultBackend = nullptr;

#define ASSERT_EQ(a, b)                             \
  if ((a) != (b)) {                                 \
    printf("assert failed at line %d\n", __LINE__); \
    printf("%lu != %lu\n", (a), (b));               \
    abort();                                        \
  }

void testOHashMapImproved(size_t mapSize = 5e6) {
  size_t round = 1e3;
  if (round > mapSize) {
    round = mapSize;
  }
  size_t initSize = mapSize;
  using ORAMClient =
      _ORAM::PathORAM::ORAMClient::ORAMClient<_OBST::Node, ORAM__Z, false, 4,
                                              24>;
  using OramClient = _OBST::OramClient::OramClient<ORAMClient>;
  uint64_t start, end;
  printf("mapSize = %u, threadCount = %d, batchSize = %u\n", mapSize, 1, 1);
  ocall_measure_time(&start);
  _OBST::OBST::OBST<OramClient> omap((uint32_t)mapSize);
  ocall_measure_time(&end);
  uint64_t timediff = end - start;
  printf("oram init time %f s\n", (double)timediff * 1e-9);

  ocall_measure_time(&start);
  for (size_t r = 0; r < round; ++r) {
    ETH_Addr addr = r;
    ERC20_Balance balance;
    omap.Insert(addr, balance);
  }
  ocall_measure_time(&end);
  timediff = end - start;
  printf("oram insert time %f us\n", (double)timediff * 1e-3 / (double)round);

  ocall_measure_time(&start);
  for (size_t r = 0; r < round; ++r) {
    ETH_Addr addr = r;
    ERC20_Balance balance;
    omap.Get(addr, balance);
  }
  ocall_measure_time(&end);
  timediff = end - start;
  printf("oram find time %f us\n", (double)timediff * 1e-3 / (double)round);

  // ocall_measure_time(&start);
  // for (size_t r = 0; r < round; ++r) {
  //   ETH_Addr addr = {};
  //   memcpy(addr.data, &r, sizeof(r));
  //   omap.OErase(addr);
  // }
  // ocall_measure_time(&end);
  // timediff = end - start;
  // printf("oram erase time %f us\n", (double)timediff * 1e-3 / (double)round);
}

void testOHashMapPerfDiffCond() {
  if (EM::Backend::g_DefaultBackend) {
    delete EM::Backend::g_DefaultBackend;
  }
  size_t BackendSize = 8e10;
  EM::Backend::g_DefaultBackend =
      new EM::Backend::MemServerBackend(BackendSize);
  for (uint32_t mapSize : {1e5, 2e5, 5e5, 1e6, 2e6, 5e6, 1e7, 2e7, 5e7, 1e8,
                           2e8, 5e8, 1e9, 2e9, 4e9}) {
    try {
      testOHashMapImproved(mapSize);
      // testOHashMapPerfSignal(mapSize);
    } catch (const std::runtime_error& e) {
      printf("Caught a runtime_error: %s\n", e.what());
    }
  }
}

void ecall_omap_perf() {
  if (EM::Backend::g_DefaultBackend) {
    delete EM::Backend::g_DefaultBackend;
  }
  size_t BackendSize = 4e9;
  EM::Backend::g_DefaultBackend =
      new EM::Backend::MemServerBackend(BackendSize);
  try {
    testOHashMapPerfDiffCond();

  } catch (std::exception& e) {
    printf("exception: %s\n", e.what());
  }
  return;
}