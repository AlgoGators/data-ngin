cmake_minimum_required(VERSION 3.14)
project(Algo_MBO)

include(FetchContent)

# Fetch the Databento library
FetchContent_Declare(
  databento
  GIT_REPOSITORY https://github.com/databento/databento-cpp
  GIT_TAG HEAD
)
FetchContent_MakeAvailable(databento)

# Add your executable and link it with Databento
add_executable(MBO main.cpp OrderBook.cpp)
target_link_libraries(MBO PRIVATE databento::databento)
