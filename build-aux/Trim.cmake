include_guard(GLOBAL)

# Trim the given string from white spaces
# @param input the string to trim
# @param output the name of the output variable where to store the result
function(trim input output)
    string(REPLACE " " ";" temp "${input}")
    list(REMOVE_ITEM temp "")
    string(REPLACE ";" " " result "${temp}")
    set("${output}" "${result}" PARENT_SCOPE)
endfunction()