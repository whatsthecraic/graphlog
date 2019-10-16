#include "output_buffer.hpp"

#include <cassert>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <mutex>
#include "lib/common/error.hpp"
#include "writer.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *  LOG & Debug                                                              *
 *                                                                           *
 *****************************************************************************/
extern std::mutex g_mutex_log;
#define LOG(msg) { std::scoped_lock xlock_log(g_mutex_log); std::cout << msg << std::endl; }

//#define DEBUG
#define COUT_DEBUG_FORCE(msg) LOG("[OutputBuffer::" << __FUNCTION__ << "] " << msg)
#if defined(DEBUG)
#define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
#define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *  OutputBuffer                                                             *
 *                                                                           *
 *****************************************************************************/

OutputBuffer::OutputBuffer(const uint64_t num_operations, Writer& writer) : m_num_operations(num_operations), m_writer(writer) {
    m_writer.open_stream_edges();
}

OutputBuffer::~OutputBuffer() noexcept (false) {
    if(m_buffer != nullptr) ERROR("Last output buffer not forwarded to the writer");
    m_writer.close_stream_edges();
}

void OutputBuffer::emit(uint64_t source, uint64_t destination, double weight){
    if(source > destination) swap(source, destination); // always store source < destination
    COUT_DEBUG("EMIT [src: " << source << ", dst: " << destination << ", weight: " << weight << "]");

    // acquire a new buffer
    if(m_buffer == nullptr){
        assert(m_index % m_writer.num_edges_per_block() == 0);
        uint64_t block_id = m_index / m_writer.num_edges_per_block();
        uint64_t num_blocks = m_num_operations / m_writer.num_edges_per_block() + (m_num_operations % m_writer.num_edges_per_block() != 0);
        assert(block_id < num_blocks);
        bool last_block = (block_id == num_blocks -1);
        m_buffer_sz = last_block ? m_num_operations - block_id * m_writer.num_edges_per_block() : m_writer.num_edges_per_block();
        m_buffer = (uint64_t*) malloc(m_buffer_sz * sizeof(uint64_t) * 3);
        if(m_buffer == nullptr) throw bad_alloc();
        m_buffer_pos = 0;
    }

    // write the edge in the buffer
    uint64_t* sources = m_buffer;
    uint64_t* destinations = sources + m_buffer_sz;
    double* weights = reinterpret_cast<double*>(destinations + m_buffer_sz);

    sources[m_buffer_pos] = source;
    destinations[m_buffer_pos] = destination;
    weights[m_buffer_pos] = weight;

    m_index++;
    m_buffer_pos++;

    // release the buffer
    if(m_buffer_pos == m_buffer_sz){
        m_writer.write_edges(reinterpret_cast<uint8_t*>(m_buffer), m_buffer_sz * sizeof(uint64_t) *3);
        m_buffer = nullptr;
        m_buffer_sz = 0;
    }
}