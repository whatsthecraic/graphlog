#pragma once

#include <cstddef>
#include <cstdint>

#include "edge.hpp"

class Writer; // forward decl.

/**
 * Output buffer for the Generator to temporarily store a sequence of operations. Once the buffer becomes full,
 * it is forwarded to the writer to be asynchronously serialised.
 */
class OutputBuffer {
    Writer& m_writer;
    const uint64_t m_num_operations; // total number of operations
    uint64_t m_index = 0; // total number of invocations to emit()
    uint64_t* m_buffer {nullptr}; // current buffer
    uint64_t m_buffer_sz = 0; // size of the current buffer, in multiples of WeightedEdges
    uint64_t m_buffer_pos = 0; // current position in the output buffer

public:
    // Initialise the class, set the total number of operations expected
    OutputBuffer(const uint64_t num_operations, Writer& writer);

    // Destructor
    ~OutputBuffer() noexcept (false);

    // Store a new edge in the buffer
    void emit(uint64_t source, uint64_t destination, double weight);
};
