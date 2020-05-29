find_path(RE2_INCLUDE_DIR
    re2/re2.h)

find_library(RE2_LIBRARY
    NAMES re2)

mark_as_advanced(RE2_LIBRARY RE2_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(re2 DEFAULT_MSG
    RE2_LIBRARY RE2_INCLUDE_DIR)

if (RE2_FOUND)
    set(RE2_INCLUDE_DIRS ${RE2_INCLUDE_DIR})
    set(RE2_LIBRARIES ${RE2_LIBRARY})
endif()
