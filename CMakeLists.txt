cmake_minimum_required(VERSION 3.19)
project(ssdiq LANGUAGES C CXX)

# ------------------------------------------------------------
# CMake 4.x compatibility:
# Some dependencies (e.g., crc32c) still declare very old
# cmake_minimum_required(VERSION < 3.5), which CMake 4.x rejects.
# This tells CMake to assume at least 3.5 for policy compatibility.
# ------------------------------------------------------------
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

# ------------------------------------------------------------
# C++ standard
# ------------------------------------------------------------
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ------------------------------------------------------------
# Includes
# ------------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR}/shared)

# ------------------------------------------------------------
# Disable tests everywhere (prevents building 3rd-party tests)
# ------------------------------------------------------------
include(CTest)  # defines BUILD_TESTING option
set(BUILD_TESTING OFF CACHE BOOL "Disable all tests" FORCE)

# Explicitly disable crc32c tests/benchmarks as well
set(CRC32C_BUILD_TESTS OFF CACHE BOOL "Disable crc32c tests" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "Disable crc32c benchmarks" FORCE)

# ------------------------------------------------------------
# Compiler flags
# ------------------------------------------------------------
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 -g -fno-omit-frame-pointer -finline-limit=1"
)
set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -fsanitize=undefined -fsanitize=address -fno-omit-frame-pointer"
)

# ------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------
# include(${CMAKE_SOURCE_DIR}/libs/liburing.cmake)
include(${CMAKE_SOURCE_DIR}/libs/crc32c.cmake)

# ------------------------------------------------------------
# Targets
# ------------------------------------------------------------
add_subdirectory(sim)
add_subdirectory(iob)
add_subdirectory(zipf)
