include_guard(GLOBAL)
include(Trim)

function(get_compiler_flags target lang result)
    # Retrieve ${CMAKE_[C|CXX]_FLAGS_[Debug|Release]
    string(TOUPPER "CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}" name)
    set(flags "${${name}}")

    # CMAKE_[C|CXX]_COMPILER_ARG1
    set(flags "${flags} ${CMAKE_${lang}_COMPILER_ARG1}")

    # Retrieve ${CMAKE_[C|CXX]_FLAGS}
    set(flags "${flags} ${CMAKE_${lang}_FLAGS}")

    trim("${flags}" flags)
    set(${result} "${flags}" PARENT_SCOPE)
endfunction()

macro(get_c_compiler_flags target flags)
    get_compiler_flags("${target}" "C" "${flags}")
endmacro()

macro(get_cxx_compiler_flags target flags)
    get_compiler_flags("${target}" "CXX" "${flags}")
endmacro()