# ---------------------------------------------------------------------------
# CLANG-TIDY
# ---------------------------------------------------------------------------

find_program(CLANG_TIDY_BIN NAMES clang-tidy-5.0 clang-tidy)

if ("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    message(WARNING "Could not find clang-tidy.")
endif()

string(CONCAT FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
)

# run clang tidy
add_custom_target(check-clang-tidy
        ${SCRIPT_DIR}/run_clang_tidy.py      # run LLVM's clang-tidy script
        -clang-tidy-binary ${CLANG_TIDY_BIN} # using our clang-tidy binary
        -p ${CMAKE_BINARY_DIR}               # using generated compile commands
        -header-filter ".*"
        -quiet
)

# run clang tidy and fix files in place.
add_custom_target(check-clang-tidy-fix
        ${SCRIPT_DIR}/run_clang_tidy.py      # run LLVM's clang-tidy script
        -clang-tidy-binary ${CLANG_TIDY_BIN} # using our clang-tidy binary
        -p ${CMAKE_BINARY_DIR}               # using generated compile commands
        -quiet
        -fix
)
