SET(INCEPTION_SOURCES
  inception.cc
  inception_context.cc
  inception_parse.cc
  inception_result.cc
  inception_audit.cc
  inception_exec.cc
  inception_backup.cc
  inception_tree.cc
  inception_log.cc
)

ADD_LIBRARY(inception_lib STATIC ${INCEPTION_SOURCES})

TARGET_LINK_LIBRARIES(inception_lib sql_main)
