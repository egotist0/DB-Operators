# ---------------------------------------------------------------------------
# CLANG-FORMAT
# ---------------------------------------------------------------------------

find_program(CLANG_FORMAT_BIN NAMES clang-format-8 clang-format)

if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(WARNING "Could not find clang-format.")
endif()

string(CONCAT FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/unit,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test/benchmark,"                
        "${CMAKE_CURRENT_SOURCE_DIR}/test/tools,"
)

# run clang format and fix files in place.
add_custom_target(check-clang-format-fix ${SCRIPT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${SCRIPT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${FORMAT_DIRS}
        --fix
        --quiet
)

# runs clang format and 
# exit with a non-zero exit code if any files need to be reformatted
add_custom_target(check-clang-format ${SCRIPT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${SCRIPT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${FORMAT_DIRS}
        --quiet
)
