# cls_tabular skyhook functions
add_library(cls_tabular SHARED cls_tabular.cc cls_tabular_utils.cc cls_tabular_processing.cc)
target_link_libraries(cls_tabular re2 arrow parquet Boost::date_time)
set_target_properties(cls_tabular PROPERTIES VERSION "1.0.0" SOVERSION "1")
install(TARGETS cls_tabular DESTINATION ${cls_dir})

# cls_tabular skyhook flatflex writer
add_executable(sky_tabular_flatflex_writer sky_tabular_flatflex_writer.cc cls_tabular_utils.cc cls_tabular_processing.cc)
target_link_libraries(sky_tabular_flatflex_writer librados global re2 arrow parquet)
install(TARGETS sky_tabular_flatflex_writer DESTINATION bin)
