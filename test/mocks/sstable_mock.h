#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "src/sstable.h"

namespace diodb {
namespace test {

class MockSSTable : public SSTable {
 public:
  MockSSTable(const fs::path sstable_path) : SSTable(sstable_path) {}
  MockSSTable(const fs::path& new_sstable_path, const Memtable& memtable)
      : SSTable(new_sstable_path, memtable) {}
  MockSSTable(const fs::path new_sstable_path,
              const std::vector<SSTablePtr>& sstables)
      : SSTable(new_sstable_path, sstables) {}
  ~MockSSTable() {}

  MOCK_CONST_METHOD0(KeyIndexOffsetBytes, off_t());
};

}  // namespace test
}  // namespace diodb
