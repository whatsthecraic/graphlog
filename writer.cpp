#include "writer.hpp"

#include <algorithm>
#include <cassert>
#include <ctime>
#include <memory>

#include "lib/common/error.hpp"
#include "lib/common/timer.hpp"
#include "zlib.h"

using namespace common;
using namespace std;
static string get_current_datetime(); // internal helper

Writer::Writer(){
    m_properties.emplace_back("internal.vertices.begin.final", "                   ");
    m_properties.emplace_back("internal.vertices.begin.temporary", "                   ");
    m_properties.emplace_back("internal.edges.begin", "                   ");
    m_properties.emplace_back("internal.edges.block_size", to_string(edges_block_size()));
}

Writer::~Writer(){ }

void Writer::set_property0(const std::string& name, const std::string& value, bool internal) {
    if(!internal && name.substr(0, 9) == "internal.") ERROR("Reserved key: " << name);

    auto it = std::find_if(begin(m_properties), end(m_properties), [name](const Property& p){ return name == p.first; });
    if( it == end(m_properties) ){
        m_properties.emplace_back(name, value);
    } else {
        it->second = value;
    }
}

void Writer::set_vertices(const uint64_t *vertices, size_t vertices_sz, size_t num_final_vertices) {
    if(num_final_vertices > vertices_sz) INVALID_ARGUMENT("vertices_sz < num_final_vertices");
    m_vertices = vertices;
    m_vertices_sz = vertices_sz;
    m_num_vertices_final = num_final_vertices;
}

void Writer::set_edges(const WeightedEdge *edges, size_t edges_sz) {
    m_edges_final = edges;
    m_edges_final_sz = edges_sz;
}

void Writer::set_operations(const Generator::OpList *operations, size_t operations_sz) {
    m_oplist = operations;
    m_oplist_sz = operations_sz;
}

size_t Writer::edges_block_size(uint64_t num_operations) const {
    return num_operations * (2*sizeof(uint64_t) + sizeof(double));
}

void Writer::save(const std::string& path){
    cout << "Saving the list of operations in `" << path << "' ..." << endl;
    Timer timer;
    timer.start();

    fstream f (path, ios_base::out | ios_base::binary);
    if(!f.good()) ERROR("Cannot open the file `" << path << "' for writing");

    uint64_t marker_vertices_temp_begin = 0;
    uint64_t marker_edges_begin = 0;

    write_metadata(f, &marker_vertices_temp_begin, &marker_edges_begin);

    write_vertices(f, 0, m_num_vertices_final);

    // set the marker for the temporary vertices
    set_marker(f, marker_vertices_temp_begin);

    if(m_num_vertices_final < m_vertices_sz) { // set the marker for the sequence of operations
        write_vertices(f, m_num_vertices_final, m_vertices_sz - m_num_vertices_final);
    }

    set_marker(f, marker_edges_begin);
    write_sequence_operations(f);

    f.close();

    timer.stop();
    cout << "Serialisation performed in " << timer << endl;
}

void Writer::write_metadata(std::fstream &file, uint64_t* out_marker_vertices_begin_temp, uint64_t* out_marker_edges_begin) {
    set_property0("internal.vertices.cardinality", to_string(m_vertices_sz), true);
    set_property0("internal.vertices.num_final", to_string(m_num_vertices_final), true);
    set_property0("internal.vertices.num_temporary", to_string(m_vertices_sz - m_num_vertices_final), true);
    set_property0("internal.edges.cardinality", to_string(m_oplist_sz), true);
    set_property0("internal.edges.num_blocks", to_string( (m_oplist_sz / NUM_OPERATIONS_PER_BLOCK) + (m_oplist_sz % NUM_OPERATIONS_PER_BLOCK != 0) ), true);
    std::sort(begin(m_properties), end(m_properties), [](const Property& p1, const Property& p2){
        return p1.first < p2.first;
    });
    streampos marker_vertices_begin_final = 0;

    file << "# GRAPHLOG\n";
    file << "# File created by `graphlog-ggu' on " << get_current_datetime() << "\n\n";

    for(auto& property : m_properties){
        file << property.first << " = ";
        if(property.first == "internal.vertices.begin.final") {
            marker_vertices_begin_final = file.tellp();
        } else if (out_marker_vertices_begin_temp != nullptr && property.first == "internal.vertices.begin.temporary"){
            *out_marker_vertices_begin_temp = file.tellp();
        } else if (out_marker_edges_begin != nullptr && property.first == "internal.edges.begin"){
            *out_marker_edges_begin = file.tellp();
        }
        file << property.second << "\n";
    }

    file << "\n__BINARY_SECTION_FOLLOWS\n";

    set_marker(file, marker_vertices_begin_final);
}

void Writer::write_vertices(std::fstream &file, uint64_t start, uint64_t length) {
    assert(start + length <= m_vertices_sz);
    cout << "Compressing and writing the list of vertices [" << start << ", " << start + length << ") ..." << endl;
    Timer timer;
    timer.start();

    int rc = 0;

    z_stream z;
    z.zalloc = Z_NULL;
    z.zfree = Z_NULL;
    z.opaque = Z_NULL;
    z.next_in = (unsigned char*) (m_vertices + start);
    z.avail_in = length * sizeof(uint64_t);
    rc = deflateInit2(&z, 9, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY);
    if(rc != Z_OK) ERROR("Cannot initialise zlib: " << z.msg);

    constexpr uint64_t buffer_sz = (1ull << 24); // 16 MB
    unique_ptr<uint8_t []> ptr_buffer { new uint8_t[buffer_sz] };
    uint8_t* buffer = ptr_buffer.get();

    do {
        z.avail_out = buffer_sz;
        z.next_out = buffer;
        rc = deflate(&z, Z_FINISH);
        assert(rc != Z_STREAM_ERROR);
        uint64_t bytes_compressed = buffer_sz - z.avail_out;
        file.write((char*) buffer, bytes_compressed);

//        cout << "[output_vertices (" << bytes_compressed << ")]: " << (int) buffer[0] << ":" << (int) buffer[1] << ":" << (int) buffer[2] << ":" << (int) buffer[3] << ":"
//             << (int) buffer[4] << ":" << (int) buffer[5] << ":" << (int) buffer[6] << ":" << (int) buffer[7] << endl;

        if(!file.good()) ERROR("Cannot write into the output stream");
    } while(z.avail_out == 0);

    rc = deflateEnd(&z);
    if(rc != Z_OK) ERROR("Cannot properly close the zlib stream: " << z.msg);

    timer.stop();
    cout << "List of vertices saved in " << timer << endl;
}

void Writer::write_sequence_operations(std::fstream& file) {
    cout << "Compressing and writing the list of edges ..." << endl;
    Timer timer;
    timer.start();

    const uint64_t input_buffer_sz = edges_block_size();
    unique_ptr<uint8_t []> ptr_input_buffer {new uint8_t[input_buffer_sz] };
    uint8_t* input_buffer = ptr_input_buffer.get();

    int rc = 0;

    // init zlib
    z_stream z;
    z.zalloc = Z_NULL;
    z.zfree = Z_NULL;
    z.opaque = Z_NULL;
    z.next_in = input_buffer;
    z.avail_in = input_buffer_sz;
    rc = deflateInit2(&z, 9, Z_DEFLATED, /* avoid header, windowBits is 2^15 */ -15, /* memLevel */ 9, Z_DEFAULT_STRATEGY);
    if(rc != Z_OK) ERROR("[rc: " << rc << "] Cannot initialise zlib: " << z.msg);

    uint64_t op_index = 0, edges_final_index = 0;
    constexpr uint64_t output_buffer_sz = 1ull << 24; // 16 MB
    unique_ptr<uint8_t []> ptr_buffer_output { new uint8_t[output_buffer_sz] };
    uint8_t* output_buffer = ptr_input_buffer.get();

    uint64_t num_blocks = (m_oplist_sz / NUM_OPERATIONS_PER_BLOCK) + (m_oplist_sz % NUM_OPERATIONS_PER_BLOCK != 0);
    for(uint64_t i = 0; i < num_blocks; i++){
        bool last_block = (i == num_blocks -1);
        uint64_t num_operations = last_block ? m_oplist_sz - op_index : NUM_OPERATIONS_PER_BLOCK;

        uint64_t* buffer_sources = reinterpret_cast<uint64_t*>(input_buffer);
        uint64_t* buffer_destinations = buffer_sources + num_operations;
        double* buffer_weights = reinterpret_cast<double*>(buffer_destinations + num_operations);

        // fill the input buffers
        for(uint64_t j = 0; j < num_operations; j++){
            buffer_sources[j] = m_vertices[ m_oplist[op_index].m_source_index ];
            buffer_destinations[j] = m_vertices[ m_oplist[op_index].m_destination_index ];
            switch(m_oplist[op_index].m_type) {
                case Generator::OpType::INSERT_FINAL_EDGE:
                    buffer_weights[j] = m_edges_final[edges_final_index].m_weight;
                    edges_final_index++;
                    break;
                case Generator::OpType::INSERT_TEMP_EDGE:
                    buffer_weights[j] = 0.0;
                    break;
                case Generator::OpType::REMOVE_TEMP_EDGE:
                    buffer_weights[j] = -1.0;
                    break;
            }

            op_index++;
        }

        // DEBUG ONLY
//        for(uint64_t j = 0; j < num_operations; j++){
//            cout << "[" << (i * NUM_OPERATIONS_PER_BLOCK) + j << "] [src: " << buffer_sources[j] << ", dst: " << buffer_destinations[j] << ", weight: " << buffer_weights[j] << "]" << endl;
//        }

        z.next_in = (unsigned char*) input_buffer;
        z.avail_in = edges_block_size(num_operations);

        do {
            z.avail_out = output_buffer_sz;
            z.next_out = output_buffer;
            rc = deflate(&z, last_block ? Z_FINISH : Z_SYNC_FLUSH);
            if(rc != Z_OK && rc != Z_STREAM_END) ERROR("Cannot compress the block");
            uint64_t bytes_compressed = output_buffer_sz - z.avail_out;

//            cout << "[output_edges (" << bytes_compressed << ")]: " << (int) output_buffer[0] << ":" << (int) output_buffer[1] << ":" << (int) output_buffer[2] << ":" << (int) output_buffer[3] << ":"
//                      << (int) output_buffer[4] << ":" << (int) output_buffer[5] << ":" << (int) output_buffer[6] << ":" << (int) output_buffer[7] << endl;

            file.write((char*) output_buffer, bytes_compressed);
            if(!file.good()) ERROR("Cannot write into the output stream");
        } while(z.avail_out == 0);
    }

    rc = deflateEnd(&z);
    if(rc != Z_OK) ERROR("Cannot properly close the zlib stream: " << z.msg);

    timer.stop();
    cout << "List of edges saved in " << timer << endl;
}

void Writer::set_marker(std::fstream& f, std::streampos marker) {
    auto marker_end = f.tellp();
    f.seekp(marker);
    f << marker_end;
    f.seekp(marker_end);
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
