# libs/crc32c.cmake
include(FetchContent)

# Use a modern crc32c version with up-to-date CMake
FetchContent_Declare(
    crc32c
    GIT_REPOSITORY https://github.com/google/crc32c.git
    GIT_TAG 1.1.2   # or newer (v1.1.2.1+ fixes CMake policy warnings)
)

FetchContent_MakeAvailable(crc32c)
