#pragma once

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lib/common/circular_array.hpp"

/**
 * Save the log of operations in the given file
 */
class Writer {
    std::fstream m_handle; // internal handle to the opened log file

    // the properties to store in the header section of the log file
    using Property = std::pair<std::string, std::string>;
    std::vector<Property> m_properties;

    // placeholder positions, to store the offsets where the vertices/edges begin in the log file
    std::streampos m_placeholder_vtx_final = 0;
    std::streampos m_placeholder_vtx_temp = 0;
    std::streampos m_placeholder_edges = 0;
    std::streampos m_placeholder_num_edges = 0; // we will know the number of operations created only at the end of the generation process

    // asynchronously compress & write block of edges to the log file
    const uint64_t m_num_compression_threads; // number of threads to use for compression
    uint64_t m_task_id = std::numeric_limits<uint64_t>::max(); // ID of the current task sent to the queue
    std::mutex m_async_mutex; // synchronisation with the background services
    std::condition_variable m_async_condvar;
    std::vector<std::thread> m_async_compressors; // handle to the background services that compresses the blocks
    std::thread m_async_writer; // handle to the background service that writes the blocks to the log file
    struct Task { uint8_t* m_buffer; uint64_t m_buffer_sz; uint64_t m_index; };
    common::CircularArray<Task> m_async_queue_c; // the queue of buffers to be compressed asynchronously
    common::CircularArray<Task> m_async_queue_w; // the queue of buffers to be written to the log file asynchronously

    // Set a property
    void set_property0(const std::string& name, const std::string& value);

    // Background service, asynchronously compress a buffer of edges
    void main_async_compress();

    // Background service, asynchronously write a block of compressed edges to the log file
    void main_async_write();

    // Write the buffer in the log file
    void write_whole_zstream(const uint8_t* buffer, uint64_t buffer_sz);

    // Write the given list of vertices
    void write_vertices(const uint64_t* vertices, uint64_t vertices_sz);

    // Set the current position in the output stream for the given placeholder
    void set_marker(std::streampos placeholder);

    // Maximum number of edge buffers that can be queued pending compression
    static constexpr uint64_t max_pending_compressions();

public:
    // Create a new instance, without creating the file yet
    Writer();

    // Destructor
    ~Writer();

    // Create the log file. After the file is created, new properties cannot be anymore added
    void create(const std::string& path_log_file);

    // Set a single property to store
    template<typename T>
    void set_property(const std::string& name, const T& value);

    // Write the final and temporary vertices in the log file
    void write_vtx_final(const uint64_t* vertices, uint64_t vertices_sz);
    void write_vtx_temp(const uint64_t* vertices, uint64_t vertices_sz);

    // The maximum number of edges to write in each block
    constexpr static uint64_t num_edges_per_block();

    // The size of each block of edges, in bytes
    constexpr static uint64_t edges_block_size();

    // Init the stream of edges
    void open_stream_edges();

    // Asynchronously write the given block of edges in the log file. Deallocate the given buffer (with ::free()) after
    // the operation has been completed
    void write_edges(uint8_t* buffer, uint64_t buffer_sz);

    // Close and flush the stream of edges to write
    void close_stream_edges();

    // Write the final number of edges stored in the file
    void write_num_edges(uint64_t num_edges);
};

// Implementation details
template<typename T>
void Writer::set_property(const std::string &name, const T &value) {
    std::stringstream ss;
    ss << value;
    set_property0(name, ss.str());
}

constexpr uint64_t Writer::num_edges_per_block() {
//    return 7; // debug only
    return (1ull << 24); // 16 M
}

constexpr uint64_t Writer::edges_block_size(){
    return num_edges_per_block() * (/* src */ sizeof(uint64_t) + /* dst */ sizeof(uint64_t) + /* weight */ sizeof(double));
}

constexpr uint64_t Writer::max_pending_compressions(){
    return 8ull;
}
