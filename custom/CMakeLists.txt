cmake_minimum_required(VERSION 3.0)

# Minimal CMake snippet for the mempool sink module.
# To include this module in build, add the source files to your target.

set(MEMPOOL_SINK_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/mempool_broadcast_sink.cpp
)

# Example: to compile into a library, uncomment and adapt for your build:
# add_library(mempool_sink ${MEMPOOL_SINK_SOURCES})
