add_executable(tests_run tests.cpp Multithreader.h Multithreader.cpp)
target_link_libraries(tests_run hashtable "-latomic")
