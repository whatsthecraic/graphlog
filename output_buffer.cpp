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

OutputBuffer::OutputBuffer(Writer& writer) : m_writer(writer) {
    m_writer.open_stream_edges();
}

OutputBuffer::~OutputBuffer() {
    flush();
    m_writer.close_stream_edges();
}

uint64_t OutputBuffer::buffer_sz() const {
    return m_writer.num_edges_per_block();
}

void OutputBuffer::emit(uint64_t source, uint64_t destination, double weight){
    if(source > destination) swap(source, destination); // always store source < destination
    COUT_DEBUG("EMIT [src: " << source << ", dst: " << destination << ", weight: " << weight << "]");

    // acquire a new buffer
    if(m_buffer == nullptr){
        m_buffer = (uint64_t*) malloc(buffer_sz() * sizeof(uint64_t) * 3);
        if(m_buffer == nullptr) throw bad_alloc();
        m_buffer_pos = 0;
    }

    // write the edge in the buffer
    uint64_t* sources = m_buffer;
    uint64_t* destinations = sources + buffer_sz();
    double* weights = reinterpret_cast<double*>(destinations + buffer_sz());

    sources[m_buffer_pos] = source;
    destinations[m_buffer_pos] = destination;
    weights[m_buffer_pos] = weight;
    m_buffer_pos++;

    // release the buffer when it's full
    if(m_buffer_pos == buffer_sz()){ flush(); }
}

void OutputBuffer::flush() {
    if(m_buffer == nullptr) return; // there are no buffers to flush

    // if we did not fill the whole buffer, we need to move ahead the columns in the expected positions
    if(m_buffer_pos < buffer_sz()){
        uint64_t* destinations_current = m_buffer + buffer_sz();
        uint64_t* destinations_expected = m_buffer + m_buffer_pos;
        memmove(destinations_expected, destinations_current, sizeof(uint64_t) * m_buffer_pos);

        double* weights_current = reinterpret_cast<double*>( destinations_current + buffer_sz() );
        double* weights_expected = reinterpret_cast<double*>( destinations_expected + m_buffer_pos );
        memmove(weights_expected, weights_current, sizeof(uint64_t) * m_buffer_pos);
    }

    m_writer.write_edges(reinterpret_cast<uint8_t*>(m_buffer), m_buffer_pos * sizeof(uint64_t) *3);
    m_buffer = nullptr;
}