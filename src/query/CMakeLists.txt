add_executable(run-query run-query.cc query.cc
    ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls/cls_tabular_utils.cc
    ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls/cls_tabular_processing.cc)

target_include_directories(run-query PRIVATE ${CMAKE_SOURCE_DIR}/src/cls/tabular/)

target_link_libraries(run-query librados global ${CMAKE_DL_LIBS}
    ${Boost_PROGRAM_OPTIONS_LIBRARY} re2 arrow)
install(TARGETS run-query DESTINATION bin)

set(UNITTEST_LIBS gmock_main gmock gtest ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
set(UNITTEST_CXX_FLAGS "-I${CMAKE_SOURCE_DIR}/src/googletest/googlemock/include -I${CMAKE_BINARY_DIR}/src/googletest/googlemock/include -I${CMAKE_SOURCE_DIR}/src/googletest/googletest/include -I${CMAKE_BINARY_DIR}/src/googletest/googletest/include -fno-strict-aliasing")


add_executable(ceph_test_skyhook_query test_query.cc query.cc
    ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls/cls_tabular_utils.cc
    ${CMAKE_SOURCE_DIR}/src/cls/tabular/cls/cls_tabular_processing.cc)

target_include_directories(ceph_test_skyhook_query
    PRIVATE ${CMAKE_SOURCE_DIR}/src/cls/tabular/)

set_target_properties(ceph_test_skyhook_query
    PROPERTIES COMPILE_FLAGS  ${UNITTEST_CXX_FLAGS})

target_link_libraries(ceph_test_skyhook_query
  librados
  global
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS}
   re2
   arrow
  )

#install(TARGETS ceph_test_skyhook_query DESTINATION ${CMAKE_INSTALL_BINDIR})
install(TARGETS ceph_test_skyhook_query DESTINATION bin)







