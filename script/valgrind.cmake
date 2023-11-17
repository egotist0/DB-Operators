# ---------------------------------------------------------------------------
# VALGRIND
# ---------------------------------------------------------------------------

find_program(VALGRIND_BIN NAMES valgrind)

if ("${VALGRIND_BIN}" STREQUAL "VALGRIND_BIN-NOTFOUND")
    message(WARNING "Could not find valgrind.")
endif()

set(VALGRIND_OPTIONS
        --error-exitcode=1        # if leaks are detected, return nonzero value
        # --gen-suppressions=all  # uncomment for leak suppression syntax
        --leak-check=full                       # detailed leak information
        --soname-synonyms=somalloc=*jemalloc*   # also intercept jemalloc
        --trace-children=yes                    # trace child processes
        --track-origins=yes       # track origin of uninitialized values
        )

set(VALGRIND_SUPPRESSIONS_FILE "${SCRIPT_DIR}/valgrind.supp")
