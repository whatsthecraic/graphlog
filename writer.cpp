#include "writer.hpp"

#include <algorithm>
#include <cassert>
#include <ctime>
#include <memory>
#include <mutex>

#include "lib/common/cpu_topology.hpp"
#include "lib/common/error.hpp"
#include "lib/common/quantity.hpp"
#include "lib/common/system.hpp"
#include "lib/common/timer.hpp"
#include "abtree.hpp"
#include "zlib.h"

using namespace common;
using namespace std;
static string get_current_datetime(); // internal helper

/*****************************************************************************
 *                                                                           *
 *  LOG & Debug                                                              *
 *                                                                           *
 *****************************************************************************/
extern std::mutex g_mutex_log;
#define LOG(msg) { std::scoped_lock xlock_log(g_mutex_log); std::cout << msg << std::endl; }

//#define DEBUG
#define COUT_DEBUG_FORCE(msg) LOG("[Writer::" << __FUNCTION__ << "] [thread_id: " << concurrency::get_thread_id() << "] " << msg)
#if defined(DEBUG)
#define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
#define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *  Initialisation                                                           *
 *                                                                           *
 *****************************************************************************/

Writer::Writer() : m_num_compression_threads(std::max<int64_t>(1, static_cast<int64_t>(cpu_topology().get_threads(false, false).size()) -2)){
    m_properties.emplace_back("internal.vertices.final.begin", "                   ");
    m_properties.emplace_back("internal.vertices.temporary.begin", "                   ");
    m_properties.emplace_back("internal.edges.begin", "                   ");
    m_properties.emplace_back("internal.edges.block_size", to_string(edges_block_size()));
    m_properties.emplace_back("internal.edges.cardinality", "                   ");
}

Writer::~Writer(){
    m_handle.close();
}

void Writer::set_property0(const std::string& name, const std::string& value) {
    if(m_handle.is_open()) ERROR("Cannot set new properties, the header was already written");

    auto it = std::find_if(begin(m_properties), end(m_properties), [name](const Property& p){ return name == p.first; });
    if( it == end(m_properties) ){
        m_properties.emplace_back(name, value);
    } else {
        it->second = value;
    }
}

void Writer::create(const std::string& path_log_file){
    if(m_handle.is_open()) ERROR("Already created");

    m_handle.open(path_log_file, ios_base::out | ios_base::binary);
    if(!m_handle.good()) ERROR("Cannot open the file `" << path_log_file << "' for writing");

    m_handle << "# GRAPHLOG\n";
    m_handle << "# File created by `graphlog-ggu' on " << get_current_datetime() << "\n\n";

    std::sort(begin(m_properties), end(m_properties), [](const Property& p1, const Property& p2){
        return p1.first < p2.first;
    });

    for(auto& property : m_properties){
        m_handle << property.first << " = ";
        if(property.first == "internal.vertices.final.begin") {
            m_placeholder_vtx_final = m_handle.tellp();
        } else if (property.first == "internal.vertices.temporary.begin"){
            m_placeholder_vtx_temp = m_handle.tellp();
        } else if (property.first == "internal.edges.begin"){
            m_placeholder_edges = m_handle.tellp();
        } else if (property.first == "internal.edges.cardinality"){
            m_placeholder_num_edges = m_handle.tellp();
        }
        m_handle << property.second << "\n";
    }

    m_handle << "\n__BINARY_SECTION_FOLLOWS\n";
}

/*****************************************************************************
 *                                                                           *
 *  Helpers                                                                  *
 *                                                                           *
 *****************************************************************************/
void Writer::set_marker(std::streampos marker) {
    auto marker_end = m_handle.tellp();
    m_handle.seekp(marker);
    m_handle << marker_end;
    m_handle.seekp(marker_end);
}

static string get_current_datetime(){
    auto t = time(nullptr);
    if(t == -1){ ERROR("Cannot fetch the current time"); }
    auto tm = localtime(&t);
    char buffer[256];
    auto rc = strftime(buffer, 256, "%d/%m/%Y %H:%M:%S", tm);
    if(rc == 0) ERROR("strftime");
    return string(buffer);
}

/*****************************************************************************
 *                                                                           *
 *  Save the vertices                                                        *
 *                                                                           *
 *****************************************************************************/

void Writer::write_vtx_final(const uint64_t* vertices, uint64_t vertices_sz){
    set_marker(m_placeholder_vtx_final);
    write_vertices(vertices, vertices_sz);
}

void Writer::write_vtx_temp(const uint64_t* vertices, uint64_t vertices_sz){
    set_marker(m_placeholder_vtx_temp);
    write_vertices(vertices, vertices_sz);
}

void Writer::write_vertices(const uint64_t* vertices, uint64_t vertices_sz){
#if defined(DEBUG)
    for(uint64_t i = 0; i < vertices_sz; i++){
        cout << "[" << i << "] vertex_id: " << vertices[i] << endl;
    }
#endif

    LOG("Compressing and saving " <<  vertices_sz  << " vertices ...")
    Timer timer;
    timer.start();
    write_whole_zstream(reinterpret_cast<const uint8_t*>(vertices), vertices_sz * sizeof(vertices[0]));
    timer.stop();
    LOG("List of vertices serialised in " << timer);
}

void Writer::write_whole_zstream(const uint8_t* buffer, uint64_t buffer_sz) {
    int rc = 0;

    z_stream z;
    z.zalloc = Z_NULL;
    z.zfree = Z_NULL;
    z.opaque = Z_NULL;
    z.next_in = (unsigned char*) buffer;
    z.avail_in = buffer_sz;
    rc = deflateInit2(&z, 9, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY);
    if(rc != Z_OK) ERROR("Cannot initialise zlib: " << z.msg << " (rc: " << rc << ")");

    constexpr uint64_t output_buffer_sz = (1ull << 24); // 16 MB
    unique_ptr<uint8_t []> ptr_output_buffer {new uint8_t[output_buffer_sz] };
    uint8_t* output_buffer = ptr_output_buffer.get();

    do {
        // invoke zlib
        z.avail_out = output_buffer_sz;
        z.next_out = output_buffer;
        rc = deflate(&z, Z_FINISH);
        assert(rc != Z_STREAM_ERROR);
        uint64_t bytes_compressed = output_buffer_sz - z.avail_out;

        // write into the file
        m_handle.write((char*) output_buffer, bytes_compressed);
        if(!m_handle.good()) ERROR("Cannot write into the output stream");
    } while(z.avail_out == 0);

    rc = deflateEnd(&z);
    if(rc != Z_OK) ERROR("Cannot properly close the zlib stream: " << z.msg);
}

/*****************************************************************************
 *                                                                           *
 *  Write edges (API)                                                        *
 *                                                                           *
 *****************************************************************************/

void Writer::open_stream_edges(){
    unique_lock<mutex> lock(m_async_mutex);
    if(m_task_id != numeric_limits<uint64_t>::max() || m_async_writer.joinable()) ERROR("Stream already initialised");

    m_task_id = 0;
    m_async_queue_c.clear();
    m_async_queue_w.clear();
    m_async_compressors.clear();

    // init the compression threads
    for(uint64_t i = 0; i < m_num_compression_threads; i++){
        m_async_compressors.emplace_back(&Writer::main_async_compress, this);
    }

    // init the writer service
    m_async_queue_w.append(Task{ nullptr, 0, 0});
    m_async_writer = std::thread{&Writer::main_async_write, this};
    m_async_condvar.wait(lock, [this](){ return m_async_queue_w.empty(); });
}

void Writer::write_edges(uint8_t* buffer, uint64_t buffer_sz){
    if(buffer == nullptr) return; /* nop */

    unique_lock<mutex> lock(m_async_mutex);
    if(!m_async_writer.joinable()) ERROR("Stream not initialised or closed");
    if(m_task_id == numeric_limits<uint64_t>::max()) ERROR("Stream closing...");

    // wait for the previous tasks to finish
    m_async_condvar.wait(lock, [this](){ return m_async_queue_c.size() < max_pending_compressions(); });
    m_async_queue_c.append( Task{ buffer, buffer_sz, m_task_id++ } );
    lock.unlock();

    m_async_condvar.notify_all();
}

void Writer::close_stream_edges() {
    uint64_t next_task_id = numeric_limits<uint64_t>::max();

    { // restrict the scope
        scoped_lock<mutex> lock{m_async_mutex};
        if (!m_async_writer.joinable()) ERROR("Stream already closed");
        // first terminate all compression threads
        for (uint64_t i = 0; i < m_num_compression_threads; i++) {
            m_async_queue_c.append(Task{nullptr, 0, 0});
        }
        std::swap(next_task_id, m_task_id);
    }

    m_async_condvar.notify_all();
    for(uint64_t i = 0; i < m_num_compression_threads; i++){
        m_async_compressors[i].join();
    }

    { // terminate the writer service
        scoped_lock<mutex> lock{m_async_mutex};
        m_async_queue_w.append(Task{nullptr, 0, next_task_id});
    }
    m_async_condvar.notify_all();
    m_async_writer.join();
}

void Writer::write_num_edges(uint64_t num_edges) {
    auto marker_end = m_handle.tellp();
    m_handle.seekp(m_placeholder_num_edges);
    m_handle << num_edges;
    m_handle.seekp(marker_end);
}

/*****************************************************************************
 *                                                                           *
 *  Compress edges (background service)                                      *
 *                                                                           *
 *****************************************************************************/
void Writer::main_async_compress() {
    COUT_DEBUG("Service started");
    common::concurrency::set_thread_name("async-compress");

    while(true) {
        Task task;
        { // fetch the next buffer from the queue
            m_async_condvar.notify_all();
            unique_lock<mutex> lock(m_async_mutex);
            m_async_condvar.wait(lock, [this](){ return !m_async_queue_c.empty(); });
            task = m_async_queue_c[0];
            m_async_queue_c.pop();
        }

        if(task.m_buffer == nullptr) break; // the driver requested the service to terminate

        // profiling information
        Timer timer; timer.start();

        // allocate a buffer to store the compressed output
        uint64_t output_buffer_sz = task.m_buffer_sz + (1ull << 20); // add 2 MB to stay on the safe side
        uint8_t* output_buffer = (uint8_t*) malloc(output_buffer_sz);
        if(output_buffer == nullptr) throw bad_alloc{}; // boom
        uint64_t input_buffer_sz = task.m_buffer_sz;
        uint8_t* input_buffer = task.m_buffer;

        // initialise the zlib stream
        z_stream z;
        z.zalloc = Z_NULL;
        z.zfree = Z_NULL;
        z.opaque = Z_NULL;
        int rc = deflateInit2(&z, 9, Z_DEFLATED, /* avoid header, windowBits is 2^15 */ -15, /* memLevel */ 9, Z_DEFAULT_STRATEGY);
        if(rc != Z_OK) ERROR("[rc: " << rc << "] Cannot initialise zlib: " << z.msg); // => BOOM, All these exceptions are unhandled

        // input buffer
        z.next_in = (unsigned char*) input_buffer;
        z.avail_in = input_buffer_sz;

        // output buffer
        z.avail_out = output_buffer_sz;
        z.next_out = output_buffer;
        rc = deflate(&z, Z_FINISH);
        if(rc != Z_STREAM_END) ERROR("Cannot compress the block in one pass");
        uint64_t bytes_compressed = output_buffer_sz - z.avail_out;
        free(input_buffer);  // deallocate the input buffer

        // close the stream
        rc = deflateEnd(&z);
        if(rc != Z_OK) ERROR("Cannot properly close the zlib stream: " << z.msg);

        // forward the task to the writer
        task.m_buffer = output_buffer;
        task.m_buffer_sz = bytes_compressed;
        {
            scoped_lock<mutex> lock(m_async_mutex);
            m_async_queue_w.append(task);
        } // #notify_all() is invoked at the next start of the loop

        timer.stop();
        LOG("Edge block of size " << ComputerQuantity( input_buffer_sz ) << "B compressed in " << ComputerQuantity(bytes_compressed) << "B "
            "(ratio: " << static_cast<double>(bytes_compressed)/input_buffer_sz << "), elapsed time: " << timer);
    }

    COUT_DEBUG("Service terminated");
}

/*****************************************************************************
 *                                                                           *
 *  Write edge blocks (background service)                                   *
 *                                                                           *
 *****************************************************************************/
void Writer::main_async_write() {
    COUT_DEBUG("Service started");
    common::concurrency::set_thread_name("async-write");
    set_marker(m_placeholder_edges);

    uint64_t next_task_id = 0;
    // tasks may arrive in a different order than what required, use the `reorder_buffer' to wait for blocks that need
    // to serialised before what received
    ABTree<uint64_t, Task> reorder_buffer { 64, 64 };

    { // awake the driver
        scoped_lock<mutex> lock(m_async_mutex);
        assert(m_async_queue_w.size() == 1);
        Task task = m_async_queue_w[0];
        m_async_queue_w.pop();
        assert(task.m_buffer == nullptr);
    }

    Task task;
    Timer timer;
    do {
        m_async_condvar.notify_all();

        // fetch the next task from the queue
        if(!reorder_buffer.find(next_task_id, &task)) {
            unique_lock<mutex> lock(m_async_mutex);
            m_async_condvar.wait(lock, [this](){ return !m_async_queue_w.empty(); });
            while(!m_async_queue_w.empty()){
                Task task_queued = m_async_queue_w[0];
                m_async_queue_w.pop();
                if(task_queued.m_index != next_task_id) {
                    reorder_buffer.insert(task_queued.m_index, task_queued);
                } else {
                    task = task_queued;
                }
            }

            if(task.m_index != next_task_id) continue;
        }

#if defined(DEBUG)
        stringstream ss;
        ss << "[" << next_task_id << "] Writing a block of " << ComputerQuantity(task.m_buffer_sz) << "B ... ";
        if(task.m_buffer_sz > 8){
            ss << "First bytes: [";
            for(uint64_t i = 0; i < 8; i++){
                if(i > 0) ss << ", ";
                ss << (int) task.m_buffer[i];
            }
            ss << "], position: " << m_handle.tellp();
        }
        COUT_DEBUG(ss.str());
#endif

        m_handle.write((char*) task.m_buffer, task.m_buffer_sz);
        free(task.m_buffer);
        next_task_id++;
    } while(task.m_buffer != nullptr);

    COUT_DEBUG("Service terminated");
}
