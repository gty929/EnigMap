#include <functional>

#include "external_memory/algorithm/ca_bucket_sort.hpp"
#include "external_memory/algorithm/min_io_sort.hpp"
#include "external_memory/algorithm/naive_bucket_sort.hpp"
#include "external_memory/algorithm/sort.hpp"

// #include "oram/common/block.hpp"
#include <gtest/gtest.h>

#include <unordered_map>

#include "testutils.hpp"

#define test_sort(n, f, ...)       \
  _test_sort<Vector<SortElement>>( \
      n, #f, [](auto&& arr) { f(arr); } __VA_OPT__(, ) __VA_ARGS__)
#define test_sort_cache(n, f, Vec, ...)                  \
  _test_sort<Vec<SortElement, 65472, true, true, 1024>>( \
      n, #f, [](auto&& arr) { f(arr); } __VA_OPT__(, ) __VA_ARGS__)
#define ENABLE_PROFILING

using namespace EM::Algorithm;
using namespace EM::NonCachedVector;
using namespace EM::MemServer;
using namespace std;

void printProfile(uint64_t N, ostream& ofs, auto& diff) {
  PERFCTR_LOG_TO(ofs);
  ofs << "N: " << N << std::endl;
  ofs << "Element size (bytes): " << sizeof(SortElement) << std::endl;
  ofs << "time (s): " << setw(9) << diff.count() << std::endl;
}

TEST(TestSort, TestSortPerfCount) {
  RELEASE_ONLY_TEST();
  printf("ecall_sort_perf called\n");
  dbg_printf("run in debug mode\n");
  for (double factor = 1; factor <= 800; factor *= 1.2) {
    uint64_t size = (uint64_t)(factor * (1UL << 19));
    if (EM::Backend::g_DefaultBackend) {
      delete EM::Backend::g_DefaultBackend;
    }
    EM::Backend::g_DefaultBackend =
        new EM::Backend::MemServerBackend((uint64_t)((1UL << 10) * size));
    Vector<SortElement> vExt(size);
    Vector<SortElement>::Writer writer(vExt.begin(), vExt.end());
    for (uint64_t i = 0; i < size; ++i) {
      SortElement element = SortElement();
      element.key = UniformRandom();
      writer.write(element);
    }
    writer.flush();
    PERFCTR_RESET();
    auto start = std::chrono::system_clock::now();
    // BucketObliviousSort(vExt.begin(), vExt.end());
    minIOSort(vExt.begin(), vExt.end());
    // BitonicSort(vExt.begin(), vExt.end());
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start;
    printf("%ld\t%f\t%ld\t%ld\t%ld\n", size, diff.count(),
           g_PerfCounters.swapCount, g_PerfCounters.readCount,
           g_PerfCounters.writeCount);
  }
}

template <typename Vec, typename F>
void _test_sort(size_t size, string funcname, F&& sortFunc,
                bool isPermutation = false) {
  delete EM::Backend::g_DefaultBackend;
  EM::Backend::g_DefaultBackend =
      new EM::Backend::MemServerBackend((1ULL << 10) * size);
  srand(time(NULL));
  const uint64_t N = size;
  cout << "test " << funcname << " perf on input size " << N << endl;
  SortElement defaultval;
  vector<SortElement> v(N, defaultval);
  unordered_map<uint64_t, int> value_count;
  uint64_t B = 4096 / 128;
  for (uint64_t i = 0; i < N; i += B) {
    // uint64_t rd =
    for (uint64_t j = i; j < i + B; ++j) {
      v[j].key = UniformRandom();
      ++value_count[v[j].key];
    }
  }
  Vec vExt(N);
  if constexpr (std::is_same<Vec, Vector<SortElement>>::value) {
    CopyOut(v.begin(), v.end(), vExt.begin());
  } else {
    std::copy(v.begin(), v.end(), vExt.begin());
  }

  auto cmp = [](const SortElement& ele1, const SortElement& ele2) {
    return ele1.key < ele2.key;
  };

  PERFCTR_RESET();
  auto start = std::chrono::system_clock::now();
  sortFunc(vExt);

  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> diff = end - start;
  printProfile(N, cout, diff);
  if constexpr (std::is_same<Vec, Vector<SortElement>>::value) {
    CopyIn(vExt.begin(), vExt.end(), v.begin(), 1);
  } else {
    std::copy(vExt.begin(), vExt.end(), v.begin());
  }

  // check it's a permutation
  for (uint64_t i = 0; i < N; ++i) {
    if (value_count[v[i].key] == 0) {
      printf("index %ld contains duplicate key %ld\n", i, v[i].key);
      Assert(false);
    }
    ASSERT_GE(--value_count[v[i].key], 0);
  }
  if (!isPermutation) {
    // check increasing order
    for (uint64_t i = 0; i < N - 1; ++i) {
      ASSERT_TRUE(v[i].key <= v[i + 1].key);
    }
  }
}

TEST(TestSort, TestKWayButterflySortPerf) {
  // RELEASE_ONLY_TEST();
  test_sort(1UL << 24, KWayButterflySort);
}

TEST(TestSort, TestKWayBucketObliviousPermutationPerf) {
  // RELEASE_ONLY_TEST();
  test_sort(1UL << 23, KWayBucketObliviousShuffle, true);
}

TEST(TestSort, TestDistriOSortPerf) {
  // RELEASE_ONLY_TEST();
  test_sort(5UL << 21, DistriOSort);
}

TEST(TestSort, TestCacheObliviouBucketSortPerf) {
  // RELEASE_ONLY_TEST();
  test_sort(1UL << 21, CABucketSort);
}

TEST(TestSort, TestCacheObliviouBucketPermutationPerf) {
  // RELEASE_ONLY_TEST();
  test_sort(1UL << 21, CABucketSort, true);
}

TEST(TestSort, TestBitonicObliviousSortPerf) {
  // RELEASE_ONLY_TEST();
  test_sort_cache(1UL << 21, BitonicSort, EM::ExtVector::Vector);
}

TEST(TestSort, TestOrShufflePerf) {
  // RELEASE_ONLY_TEST();
  test_sort_cache(1UL << 21, OrShuffle, EM::ExtVector::Vector, true);
}

TEST(TestSort, TestBitonicShufflePerf) {
  // RELEASE_ONLY_TEST();
  test_sort_cache(1UL << 21, BitonicShuffle, EM::ExtVector::Vector, true);
}

TEST(TestSort, TestSequentialPassPerf) {
  RELEASE_ONLY_TEST();
  srand(time(NULL));
  delete EM::Backend::g_DefaultBackend;
  EM::Backend::g_DefaultBackend =
      new EM::Backend::MemServerBackend((1ULL << 34));

  uint64_t logN = 24;
  const uint64_t N = 1UL << logN;
  cout << "test bucket oblivious permutation perf " << N << endl;
  cout << logN << '\t' << N << '\t';
  SortElement defaultval;
  vector<SortElement> v(N, defaultval);
  Vector<SortElement> vExt(N);
  PERFCTR_RESET();
  auto start = std::chrono::system_clock::now();

  CopyOut(v.begin(), v.end(), vExt.begin());
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> diff = end - start;
  printProfile(N, cout, diff);
  PERFCTR_RESET();
  start = std::chrono::system_clock::now();
  uint64_t xorsum = 0;
  CopyIn(vExt.begin(), vExt.end(), v.begin());
  end = std::chrono::system_clock::now();
  diff = end - start;
  std::cout << xorsum << std::endl;
  printProfile(N, cout, diff);
}

TEST(TestSort, TestMergeSplitPerf) {
  RELEASE_ONLY_TEST();
  PERFCTR_RESET();
  auto start = std::chrono::system_clock::now();
  uint64_t bitMask = 4UL;
  for (uint64_t Z = 512; Z <= 4096; Z *= 2) {
    TaggedT<SortElement> defaultVal;
    defaultVal.setDummy();
    size_t bucketCount = 2000;
    const uint64_t N = 2 * Z * (bucketCount / 2);
    vector<TaggedT<SortElement>> v(N, defaultVal);

    for (int round = 0; round < bucketCount / 2; ++round) {
      MergeSplitTwoWay(v.begin() + 2 * round, v.begin() + 2 * round + 1, Z,
                       bitMask);
    }
  }
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> diff = end - start;
  std::cout << "time (s): " << setw(9) << diff.count() << std::endl;

  PERFCTR_LOG();
}

TEST(TestSort, TestKWayMergeSplitPerf) {
  RELEASE_ONLY_TEST();
  PERFCTR_RESET();
  for (uint64_t Z = 256; Z <= 16384; Z *= 2) {
    for (size_t way = 2; way <= 8; ++way) {
      auto start = std::chrono::system_clock::now();
      TaggedT<SortElement> defaultVal;
      defaultVal.setDummy();
      size_t bucketCount = 2000;
      const uint64_t N = way * Z * (bucketCount / way);
      vector<TaggedT<SortElement>> v(N, defaultVal);
      for (int round = 0; round < bucketCount / way; ++round) {
        vector<vector<TaggedT<SortElement>>::iterator> begins;
        for (size_t i = 0; i < way; ++i) {
          begins.push_back(v.begin() + i * Z + Z * round * way);
        }
        MergeSplitKWay(begins, Z);
      }

      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end - start;
      std::cout << "way=" << way << " Z=" << Z << " time (s): " << setw(9)
                << diff.count() << std::endl;
      // PERFCTR_LOG();
    }
    //   std::cout << diff.count() << ", ";
    // }
    // std::cout << endl;
  }
}

TEST(TestSort, TestKWayInterleaveSepMarksPerf) {
  RELEASE_ONLY_TEST();
  PERFCTR_RESET();
  for (uint64_t Z = 256; Z <= 16384; Z *= 2) {
    for (size_t k = 3; k <= 8; ++k) {
      auto start = std::chrono::system_clock::now();

      TaggedT<SortElement> defaultVal;
      defaultVal.setDummy();

      const uint64_t N = k * Z;
      vector<TaggedT<SortElement>> v(N, defaultVal);
      for (int round = 0; round < 2000 / k; ++round) {
        const auto getMark = [k = k](const auto& a) {
          return (uint16_t)(a.tag) % k;
        };
        vector<uint8_t> marks(N);
        for (size_t i = 0; i < N; ++i) {
          marks[i] = getMark(v[i]);
        }
        Interleave(v.begin(), v.end(), marks.begin(), marks.end(), k);
      }
      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end - start;
      std::cout << "k=" << k << " Z=" << Z << " time (s): " << setw(9)
                << diff.count() << std::endl;
      // PERFCTR_LOG();
    }
  }
}

TEST(TestSort, BNSort) {
  constexpr int i = 8;
  PERFCTR_RESET();
  vector<uint64_t> v(i);
  vector<uint8_t> marks(i);
  static constexpr StaticSort<i> staticSort;
  auto iter_pair = std::make_pair(v.begin(), marks.begin());
  staticSort(iter_pair);
  PERFCTR_LOG();
}

TEST(TestSort, TestMergeSplitPerfForDifferentBlockSizes) {
  RELEASE_ONLY_TEST();

  size_t Zbegin = 200, Zend = 2000;
  uint64_t runtime[4][Zend - Zbegin];
  uint64_t bitMask = 4UL;
  for (uint64_t Z = Zbegin; Z < Zend; ++Z) {
    TaggedT<uint64_t> defaultVal;
    defaultVal.setDummy();

    const uint64_t N = 2 * Z;
    vector<TaggedT<uint64_t>> v(N, defaultVal);
    for (int method = 0; method != 4; ++method) {
      // auto start = std::chrono::system_clock::now();
      // for (int round = 0; round < 10000; ++round) {
      PERFCTR_RESET();
      MergeSplitInPlace(v.begin(), v.end(), bitMask, (PartitionMethod)method);
      // }
      // auto end = std::chrono::system_clock::now();
      // std::chrono::duration<double> diff = end - start;
      runtime[method][Z - Zbegin] = g_PerfCounters.swapCount;
    }
  }
  ofstream ofs;
  ofs.open("mergesplitperf.out");
  ofs << "\tEvenOdd\tOrCompact\tGoodrich\tBitonic\t" << std::endl;
  for (uint64_t Z = Zbegin; Z < Zend; ++Z) {
    ofs << Z << '\t';
    for (int method = 0; method != 4; ++method) {
      ofs << runtime[method][Z - Zbegin] << "\t";
    }
    ofs << std::endl;
  }
  ofs.close();

  // PERFCTR_LOG();
}