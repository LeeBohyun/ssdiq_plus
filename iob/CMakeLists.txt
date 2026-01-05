file(GLOB_RECURSE SRC_FILES *.cpp)

add_executable(iob iob.cpp ${SRC_FILES})
target_link_libraries(iob -luring -laio -lnvme crc32c)
#target_include_directories(iob PRIVATE ${crc32c_SOURCE_DIR}/include)
