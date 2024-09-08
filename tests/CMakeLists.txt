include(FetchContent)

FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip)
FetchContent_MakeAvailable(googletest)

add_executable(mini_test
        Catalog_test.cpp
        HeapPageId_test.cpp
        HeapPageRead_test.cpp
        RecordId_test.cpp
        TupleDesc_test.cpp
        Tuple_test.cpp
        BTreeFile_test.cpp
        Bufferpool_test.cpp
        IntHistogram_test.cpp
)
target_link_libraries(mini_test PRIVATE GTest::gtest_main db)

include(GoogleTest)
gtest_add_tests(TARGET mini_test TEST_LIST tests)
foreach (test ${tests})
    set_tests_properties(${test} PROPERTIES TIMEOUT 5)
endforeach ()
