include_guard(GLOBAL)
include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

# Test whether the current compiler
# @param flag: the flag to add, e.g. -march=native
function(append_compiler_flag flag)
    string(MAKE_C_IDENTIFIER ${flag} flag_variable)
    set(variable_c "HAVE_C${flag_variable}")
    set(variable_cxx "HAVE_CXX${flag_variable}")
    set(only_lang "${ARGV1}") # optional

    # Current languages
    get_property(languages GLOBAL PROPERTY ENABLED_LANGUAGES)

    # Append the flag for the C compiler
    if(NOT only_lang OR "${only_lang}" STREQUAL "C")
        if(NOT CMAKE_C_FLAGS MATCHES "${flag}" AND "C" IN_LIST languages)
            check_c_compiler_flag("${flag}" "${variable_c}")
            if(${${variable_c}})
                set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${flag}" CACHE INTERNAL "")
            endif()
        endif()
    endif()

    # Append the flag for the C++ compiler
    if(NOT only_lang OR "${only_lang}" STREQUAL "CXX")
        if(NOT CMAKE_CXX_FLAGS MATCHES "${flag}" AND "CXX" IN_LIST languages)
            check_cxx_compiler_flag("${flag}" "${variable_cxx}")
            if(${${variable_cxx}})
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${flag}" CACHE INTERNAL "")
            endif()
        endif()
    endif()
endfunction()