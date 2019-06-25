#include <thread>
#include <chrono>

#include "gtest/gtest.h"

#include "src/buffer.h"
#include "src/db_controller.h"

using namespace std;

namespace diodb {
namespace test {

class DBControllerIntegrationTest : public ::testing::Test {
};

TEST_F(DBControllerIntegrationTest, Basic) {
  DBController dbcontroller(fs::path("basic_dbc_test"));
  dbcontroller.Start();
  this_thread::sleep_for(chrono::seconds(2));

  Buffer key1({'a'});
  Buffer val1({'f', 'o', 'o'});

  Buffer key2({'b'});
  Buffer val2({'b', 'a', 'r'});

  Buffer tmpk = key1;
  Buffer tmpv = val1;
  dbcontroller.Put(move(tmpk), move(tmpv));

  ASSERT_EQ(dbcontroller.Get(key1), val1);
  this_thread::sleep_for(chrono::seconds(2));
  ASSERT_EQ(dbcontroller.Get(key1), val1);
}

}  // namespace test
}  // namespace diodb
