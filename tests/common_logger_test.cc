#include "raftkv/common/logger.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace raftkv {
namespace {

class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Logger& logger = Logger::Instance();
    logger.SetOutput(&stream_);
    logger.SetMinLevel(LogLevel::kDebug);
    logger.SetNodeId("test-node");
  }

  void TearDown() override {
    Logger& logger = Logger::Instance();
    logger.SetOutput(nullptr);
    logger.SetMinLevel(LogLevel::kInfo);
    logger.SetNodeId("-");
  }

  std::ostringstream stream_;
};

TEST_F(LoggerTest, LevelNamesAreStable) {
  EXPECT_STREQ(LogLevelName(LogLevel::kDebug), "DEBUG");
  EXPECT_STREQ(LogLevelName(LogLevel::kInfo), "INFO");
  EXPECT_STREQ(LogLevelName(LogLevel::kWarn), "WARN");
  EXPECT_STREQ(LogLevelName(LogLevel::kError), "ERROR");
  EXPECT_STREQ(LogLevelName(static_cast<LogLevel>(99)), "UNKNOWN");
}

TEST_F(LoggerTest, MinimumLevelFiltersMessages) {
  Logger::Instance().SetMinLevel(LogLevel::kWarn);

  RAFTKV_LOG_DEBUG("debug");
  RAFTKV_LOG_INFO("info");
  RAFTKV_LOG_WARN("warn");
  RAFTKV_LOG_ERROR("error");
  Logger::Instance().Flush();

  const std::string output = stream_.str();
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'), 2);
  EXPECT_EQ(output.find("][DEBUG]["), std::string::npos);
  EXPECT_EQ(output.find("][INFO]["), std::string::npos);
  EXPECT_NE(output.find("][WARN]["), std::string::npos);
  EXPECT_NE(output.find("][ERROR]["), std::string::npos);
}

TEST_F(LoggerTest, FormatContainsTimestampNodeThreadAndLocation) {
  RAFTKV_LOG_INFO("formatted message");

  const std::string output = stream_.str();
  const std::regex pattern(
      R"(^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\]\[INFO\]\[node=test-node\]\[tid=[0-9]+\]\[common_logger_test\.cc:\d+\] formatted message\n$)");
  EXPECT_TRUE(std::regex_match(output, pattern));
}

TEST_F(LoggerTest, ConcurrentMessagesRemainWholeLines) {
  constexpr int kThreads = 8;
  constexpr int kMessagesPerThread = 100;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int thread_id = 0; thread_id < kThreads; ++thread_id) {
    threads.emplace_back([thread_id] {
      for (int sequence = 0; sequence < kMessagesPerThread; ++sequence) {
        RAFTKV_LOG_INFO("thread " + std::to_string(thread_id) + " message " +
                        std::to_string(sequence));
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }

  const std::string output = stream_.str();
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'),
            kThreads * kMessagesPerThread);
  for (int thread_id = 0; thread_id < kThreads; ++thread_id) {
    for (int sequence = 0; sequence < kMessagesPerThread; ++sequence) {
      const std::string message = "thread " + std::to_string(thread_id) +
                                  " message " + std::to_string(sequence);
      EXPECT_NE(output.find(message), std::string::npos);
    }
  }
}

}  // namespace
}  // namespace raftkv
