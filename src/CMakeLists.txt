set(cls_dir ${CMAKE_INSTALL_LIBDIR}/rados-classes)

# cls_sdk
add_library(cls_sdk SHARED cls_sdk.cc)
set_target_properties(cls_sdk PROPERTIES
  VERSION "1.0.0"
  SOVERSION "1"
  INSTALL_RPATH ""
  CXX_VISIBILITY_PRESET hidden)
install(TARGETS cls_sdk DESTINATION ${cls_dir})
