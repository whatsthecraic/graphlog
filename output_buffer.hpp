/**
 * Copyright (C) 2019 Dean De Leo, email: hello[at]whatsthecraic.net
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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
