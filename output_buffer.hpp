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
    uint64_t* m_buffer {nullptr}; // current buffer
    uint64_t m_buffer_pos = 0; // current position in the output buffer

private:
    // max capacity of an allocated buffer, in multiples of WeightedEdges
    uint64_t buffer_sz() const;

public:
    // Initialise the class, provide the actual writer instance to compress & save to the disk the chunks created
    OutputBuffer(Writer& writer);

    // Destructor
    ~OutputBuffer();

    // Store a new edge in the buffer
    void emit(uint64_t source, uint64_t destination, double weight);

    // Flush the last buffer to the writer
    void flush();
};
