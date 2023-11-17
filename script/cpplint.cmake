# ---------------------------------------------------------------------------
# CPPLINT
# ---------------------------------------------------------------------------

file(GLOB_RECURSE LINT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cc"
)

add_custom_target(check-cpplint echo '${LINT_FILES}' | xargs -n12 -P8
        ${SCRIPT_DIR}/cpplint.py
        --verbose=2 --quiet
        --linelength=120
        --filter=-legal/copyright,-build/header_guard
)