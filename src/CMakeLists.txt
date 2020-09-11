set(cls_dir ${CMAKE_INSTALL_LIBDIR}/rados-classes)

# cls_sdk
add_library(cls_sdk SHARED cls_sdk.cc)
set_target_properties(sdk PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden
  )

install(TARGETS cls_sdk DESTINATION ${cls_dir})

# cls_sdk_test
add_executable(cls_sdk_test
  cls_sdk_test.cc
  test_utils.cc
  )

target_link_libraries(cls_sdk_test
  ${LIBRADOS_LIBRARIES}
  ${GTEST_MAIN_LIBRARIES}
  ${GTEST_INCLUDE_DIRS}	
  ${GTEST_LIBRARIES}
  )

install(TARGETS
  cls_sdk_test
  RUNTIME DESTINATION bin
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
