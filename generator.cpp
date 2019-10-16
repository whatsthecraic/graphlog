#include "generator.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "lib/common/permutation.hpp"
#include "lib/common/timer.hpp"
#include "abtree.hpp"
#include "graphalytics_reader.hpp"
#include "output_buffer.hpp"
#include "writer.hpp"

using namespace common;
using namespace std;

/*****************************************************************************
 *                                                                           *
 *  LOG & Debug                                                              *
 *                                                                           *
 *****************************************************************************/
extern std::mutex g_mutex_log;
#define LOG(msg) { std::scoped_lock xlock_log(g_mutex_log); std::cout << msg << std::endl; }

//#define DEBUG
#define COUT_DEBUG_FORCE(msg) LOG("[Generator::" << __FUNCTION__ << "] " << msg)
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

namespace {
// Vertex-id & frequency of each vertex
struct InitVertexRecord {
    uint32_t m_offset;
    uint32_t m_frequency;
};
}

Generator::Generator(const std::string& path_input_graph, const std::string& path_output_log, Writer& writer, double sf_frequency, double ef_vertices, double ef_edges, double aging_factor, uint64_t seed) :
    m_writer(writer), m_num_operations(0), m_seed(seed), m_random(m_seed){
    unordered_map<uint64_t, InitVertexRecord> map_frequencies;
    unique_ptr<WeightedEdge[]> ptr_weighted_edges;

    init_read_input_graph(&ptr_weighted_edges, &map_frequencies, path_input_graph, ef_vertices);

    m_num_max_edges = ef_edges * m_num_edges_final;
    m_num_operations = aging_factor * m_num_edges_final;

    unique_ptr<InitVertexRecord[]> array_frequencies { new InitVertexRecord[num_vertices()] };
    init_temporary_vertices(&map_frequencies, array_frequencies.get(), sf_frequency);
    init_counting_tree(array_frequencies.get());

    init_permute_edges_final(ptr_weighted_edges);

    init_writer(path_output_log);
}

Generator::~Generator(){
    delete m_frequencies; m_frequencies = nullptr;
    delete[] m_vertices; m_vertices = nullptr;

    if(m_edges_final != nullptr){
        for(uint64_t i = 0, end = num_blocks_in_final_edges(); i < end; i++){
            free(m_edges_final[i]); m_edges_final[i] = nullptr;
        }
        free(m_edges_final);
        m_edges_final = nullptr;
    }
}

void Generator::init_read_input_graph(void* ptr_array_edges, void* ptr_frequencies, const std::string& path_input_graph, double expansion_factor_vertices) {
    LOG("Reading the input graph from: " << path_input_graph << " ... ");
    Timer timer;
    timer.start();

    assert(ptr_array_edges != nullptr);
    auto& ptr_edges_final = *reinterpret_cast<unique_ptr<WeightedEdge[]>*>(ptr_array_edges);
    assert(ptr_frequencies != nullptr);
    auto& frequencies = *reinterpret_cast<unordered_map<uint64_t, InitVertexRecord>*>(ptr_frequencies);

    GraphalyticsReader reader{path_input_graph};
    if(reader.is_directed()) ERROR("Only undirected graphs are supported. The input graph `" << path_input_graph << "' is directed");

    string prop_num_vertices = reader.get_property("meta.vertices");
    m_num_vertices_final = stoi(prop_num_vertices);
    string prop_num_edges = reader.get_property("meta.edges");
    m_num_vertices_temporary = ceil( (expansion_factor_vertices - 1.0) * m_num_vertices_final );
    if(num_vertices() > std::numeric_limits<uint32_t>::max()) {
        ERROR("Too many vertices: " << num_vertices() << ", vertices in the final graph: " << num_final_vertices() << ", expansion factor: " << expansion_factor_vertices);
    }

    m_num_edges_final = stoi(prop_num_edges);
    COUT_DEBUG("num vertices final graph: " << num_final_vertices() << ", num edges final graph: " << m_num_edges_final);

    m_vertices = new uint64_t[num_vertices()];
    ptr_edges_final.reset( new WeightedEdge[m_num_edges_final] );
    WeightedEdge* __restrict edges_final = ptr_edges_final.get();

    uint32_t vertex_next = 0;
    uint64_t edge_next = 0;
    uint64_t vertex, source, destination;
    double weight;

    while(reader.read_vertex(vertex)){
        m_vertices[vertex_next] = vertex;
        frequencies[vertex] = InitVertexRecord{vertex_next, 0};
        vertex_next++;
    }

    while(reader.read_edge(source, destination, weight)){
        assert(source != destination && "The edge has the same source & destination");
        assert(frequencies.count(source) > 0 && "This vertex is not present in the vertex list");
        assert(frequencies.count(destination) > 0 && "This vertex is not present in the vertex list");

        frequencies[source].m_frequency++;
        frequencies[destination].m_frequency++;

        uint32_t src_id = frequencies[source].m_offset;
        uint32_t dst_id = frequencies[destination].m_offset;
        assert(src_id != dst_id);
        if(dst_id < src_id) swap(src_id, dst_id);

        assert(edge_next < m_num_edges_final);
        edges_final[edge_next] = WeightedEdge{ src_id, dst_id, weight };
        edge_next++;
    }
    m_num_vertices_final = vertex_next; // actual number of vertices read in the final graph
    m_num_edges_final = edge_next; // actual number of edges read from the final graph
    cout << "The final graph will contain " << num_final_vertices() << " vertices and " << m_num_edges_final << " edges" << endl;

    timer.stop();
    LOG("Input graph parsed in " << timer);
}

void Generator::init_temporary_vertices(void* ptr_map_frequencies, void* ptr_array_frequencies, double sf_frequency){
    LOG("Generating " << num_temporary_vertices() << " (" << 100.0 * num_temporary_vertices() / num_vertices() << " %) non final vertices ... ");
    Timer timer;
    timer.start();

    assert(ptr_map_frequencies != nullptr && ptr_array_frequencies != nullptr);
    auto& map_frequencies = *reinterpret_cast<unordered_map<uint64_t, InitVertexRecord>*>(ptr_map_frequencies);
    InitVertexRecord* __restrict array_frequencies = reinterpret_cast<InitVertexRecord*>(ptr_array_frequencies);

    { // restrict the scope
        uint64_t i = 0;
        for(const auto& it : map_frequencies){
            assert(m_vertices[it.second.m_offset] == it.first);
            array_frequencies[i] = it.second;
            array_frequencies[i].m_frequency *= sf_frequency;
            i++;
        }
    }

    if(num_temporary_vertices() > 0){
        std::sort(array_frequencies, array_frequencies + num_final_vertices(), [](const InitVertexRecord& v1, const InitVertexRecord& v2){
           return v1.m_frequency > v2.m_frequency;
        });

        uint64_t external_vertex_id = 1;
        uint32_t offset_vertex_id = num_final_vertices();

        int64_t pos_tail = num_vertices() -1;
        int64_t pos_head = num_final_vertices() -1;
        uint64_t remaining_free_spots = num_temporary_vertices();
        while(remaining_free_spots > 0 && pos_tail > 0){
            assert(pos_head >= 0);
            if(remaining_free_spots * num_vertices() >= num_temporary_vertices() * pos_tail){

                // interpolate the frequency w.r.t. the two neighbours
                uint64_t vertex_freq = array_frequencies[pos_head].m_frequency;
                if(pos_tail < num_vertices() -1){
                    vertex_freq = (vertex_freq + array_frequencies[pos_tail +1].m_frequency) /2;
                }
                array_frequencies[pos_tail] = InitVertexRecord{offset_vertex_id, (uint32_t) vertex_freq};
                remaining_free_spots--;

                // generate the ID of the vertex to insert
                while(map_frequencies.count(external_vertex_id) > 0){ external_vertex_id ++ ; }
                m_vertices[offset_vertex_id] = external_vertex_id;
//                COUT_DEBUG("Temporary vertex: " << external_vertex_id << " [internal id: " << offset_vertex_id << "], frequency: " << vertex_freq);

                offset_vertex_id++;
                external_vertex_id++;
            } else {
                array_frequencies[pos_tail] = array_frequencies[pos_head];
                pos_head--;

//                COUT_DEBUG("Final vertex: " << m_vertices[array_frequencies[pos_tail].m_offset] << " [internal id: " << array_frequencies[pos_tail].m_offset << "], frequency: " << array_frequencies[pos_tail].m_frequency);
            }

            pos_tail--;
        }
    }

    timer.stop();
    LOG("Vertices generated in " << timer);
}

void Generator::init_counting_tree(void* ptr_array_frequencies){
    LOG("Initialising the counting tree for " << num_vertices() << " vertices ... ");
    Timer timer;
    timer.start();

    assert(ptr_array_frequencies != nullptr);
    InitVertexRecord* __restrict array_frequencies = reinterpret_cast<InitVertexRecord*>(ptr_array_frequencies);

    m_frequencies = new CountingTree(num_vertices());
    for(uint64_t i = 0, sz = num_vertices(); i < sz ; i++){
        m_frequencies->set(array_frequencies[i].m_offset, array_frequencies[i].m_frequency);
    }

//    m_frequencies->dump();

    timer.stop();
    LOG("Counting tree created in " << timer);
}

void Generator::init_permute_edges_final(std::unique_ptr<WeightedEdge[]>& ptr_edges_final){
    LOG("Permuting the edges in the final graph ... ");
    Timer timer;
    timer.start();

    unique_ptr<uint64_t[]> ptr_permutation { new uint64_t[m_num_edges_final] };
    uint64_t* permutation = ptr_permutation.get();
    for(uint64_t i = 0; i < m_num_edges_final; i++){
        permutation[i] = i;
    }
    common::permute(permutation, m_num_edges_final, m_seed + 57);

    WeightedEdge* edges = ptr_edges_final.get();

    uint64_t num_blocks = num_blocks_in_final_edges();
    m_edges_final = (WeightedEdge**) calloc(num_blocks, sizeof(WeightedEdge*));
    if(m_edges_final == nullptr) throw bad_alloc();

    for(uint64_t i = 0; i < num_blocks; i++){
        bool last_block = (i == num_blocks -1);
        uint64_t num_edges_in_block = last_block ? m_num_edges_final - i * m_num_final_edges_per_block : m_num_final_edges_per_block;
        m_edges_final[i] = (WeightedEdge*) malloc(num_edges_in_block * sizeof(WeightedEdge));
        if(m_edges_final[i] == nullptr) throw bad_alloc();

        for(uint64_t j = 0; j < num_edges_in_block; j++){
            m_edges_final[i][j] = edges[permutation[i * m_num_final_edges_per_block + j]];
        }
    }

    timer.stop();
    LOG("Permutation completed in " << timer);
}

void Generator::init_writer(const string& path_output){
    LOG("Initialising the log file ....");
    Timer timer;
    timer.start();

    m_writer.set_property("internal.edges.cardinality", m_num_operations);
    m_writer.set_property("internal.edges.final", num_edges());
    m_writer.set_property("internal.edges.num_blocks", num_blocks_in_operations());
    m_writer.set_property("internal.vertices.cardinality", num_vertices());
    m_writer.set_property("internal.vertices.final.cardinality", num_final_vertices());
    m_writer.set_property("internal.vertices.temporary.cardinality", num_temporary_vertices());

    m_writer.create(path_output);
    m_writer.write_vtx_final(m_vertices, num_final_vertices());
    m_writer.write_vtx_temp(m_vertices + num_final_vertices(), num_temporary_vertices());

    timer.stop();
    LOG("Log file initialised in " << timer);
}

/*****************************************************************************
 *                                                                           *
 *  Properties                                                               *
 *                                                                           *
 *****************************************************************************/

uint64_t Generator::num_blocks_in_final_edges() const {
    return (m_num_edges_final / m_num_final_edges_per_block) + (m_num_edges_final % m_num_final_edges_per_block != 0);
}

uint64_t Generator::num_blocks_in_operations() const {
    return (m_num_operations / m_writer.num_edges_per_block()) + (m_num_edges_final % m_writer.num_edges_per_block() != 0);
}

/*****************************************************************************
 *                                                                           *
 *  Generate the operations                                                  *
 *                                                                           *
 *****************************************************************************/
void Generator::generate() {
    cout << "Generating " << m_num_operations << " operations ..." << endl;
    Timer timer;
    timer.start();

    ABTree<uint64_t, Edge> temporary_edges; // edges that need to be removed before the end of the generation process
    unordered_map<Edge, uint64_t> edges_stored; // edges currently stored in the graph
    OutputBuffer output{m_num_operations, m_writer}; // output buffer
//    uniform_real_distribution<double> unif_real{0., 1.}; // uniform distribution in [0, 1]
    uniform_int_distribution<uint64_t> unif_uint64_t{1, numeric_limits<uint64_t>::max()};
    uniform_int_distribution<uint64_t> unif_frequencies{0, (uint64_t) m_frequencies->total_count() - 1};

    int last_progress_reported = 0;
    int64_t edges_final_block = -1, edges_final_offset = 0, edges_final_block_sz = 0, edges_final_position = 0;
//    double prob_bump = 1.0; // heuristics to bump up the probability of inserting a final edge
    uint64_t num_ops_performed = 0;

    while (num_ops_performed < m_num_operations) {
        assert(edges_final_position <= m_num_edges_final);
        uint64_t num_missing_final_edges = m_num_edges_final - edges_final_position;

        // Report progress
        if (static_cast<int>(100.0 * num_ops_performed / m_num_operations) > last_progress_reported) {
            last_progress_reported = 100.0 * num_ops_performed / m_num_operations;
            LOG("Progress: " << num_ops_performed << "/" << m_num_operations << " (" << last_progress_reported<< " %), "
                     "edges final: " << edges_final_position << "/" << m_num_edges_final << " (" << 100.0 * edges_final_position / m_num_edges_final << " %), "
                     "edges temp: " << temporary_edges.size() << "/" << edges_stored.size() << " (" << 100.0 * temporary_edges.size() / edges_stored.size() << " %), "
                     "ht size: " << edges_stored.size() << " (ff: " << 100.0 * edges_stored.load_factor() << " %), "
                     "abtree footprint: " << temporary_edges.memory_footprint() / 1024 / 1024 << " MB, "
                     "elapsed time: " << timer
             );
        }

        // shall we perform an insertion or a deletion ?
        if (temporary_edges.empty() || (edges_stored.size() < m_num_max_edges &&
            (num_missing_final_edges > 0 && num_ops_performed + num_missing_final_edges + temporary_edges.size() <= m_num_operations))) {
            // this is an insertion, okay. Then should it be a final or a temporary edge?
//            double prob_insert_final = prob_bump * static_cast<double>(num_missing_final_edges) / (m_num_operations - num_ops_performed);
            if ((num_ops_performed + num_missing_final_edges + temporary_edges.size() == m_num_operations) ||
                (edges_final_position < (static_cast<double>(num_ops_performed) / m_num_operations) * m_num_edges_final) ){

                // retrieve the next block of final edges
                if (edges_final_offset >= edges_final_block_sz) {
                    if (edges_final_block >= 0) {
                        COUT_DEBUG("Deallocating a block of final edges " << edges_final_block << "/" << num_blocks_in_final_edges() << " ...");
                        free(m_edges_final[edges_final_block]);
                        m_edges_final[edges_final_block] = nullptr;
                    }

                    edges_final_block++;
                    bool last_block = (edges_final_block == num_blocks_in_final_edges() - 1);
                    edges_final_block_sz = (last_block ? m_num_edges_final - edges_final_block * m_num_final_edges_per_block : m_num_final_edges_per_block);
                    edges_final_offset = 0;
                }

                // insert a final edge
                WeightedEdge edge_final = m_edges_final[edges_final_block][edges_final_offset];
                edges_final_position++;
                edges_final_offset++;

                // if we previously inserted this edge as a temporary edge, remove it first
                auto it = edges_stored.find(edge_final.edge());
                if (it != edges_stored.end()) {
                    assert(it->second > 0 && "0 is reserved for the final edges. If it's already present, then the loaded graph has duplicate edges");
                    // The (a,b)-tree may contain multiple edges with the same key (duplicates). In case we removed the
                    // wrong edge, reinsert it with a new key
                    Edge edge_removed;
                    while (temporary_edges.remove(it->second, &edge_removed) && edge_removed != edge_final.edge()) {
                        uint64_t new_key = unif_uint64_t(m_random);
                        temporary_edges.insert(new_key, edge_removed);
                        edges_stored[edge_removed] = new_key;
                    };
                    assert(edge_removed == edge_final.edge() && "Cannot find the previous temporary edge");

                    // emit a deletion
                    output.emit(m_vertices[ edge_final.source() ], m_vertices[ edge_final.destination() ], -1);
                    num_ops_performed++;

                };

                output.emit(m_vertices[ edge_final.source() ], m_vertices[ edge_final.destination() ], edge_final.weight());
                edges_stored[edge_final.edge()] = 0;
            } else { // insert a temporary edge
                // generate a random edge
                Edge edge_temporary;
                do {
                    // generate the source_id
                    uint32_t src_id = m_frequencies->search(unif_frequencies(m_random));
                    int64_t old_frequency;
                    m_frequencies->unset(src_id, &old_frequency);

                    // generate the destination_id
                    uniform_int_distribution<uint64_t> unif_tmp{0, (uint64_t) m_frequencies->total_count() - 1};
                    uint32_t dst_id = m_frequencies->search(unif_tmp(m_random));
                    assert(src_id != dst_id);

                    // reset the frequency for the source_id
                    m_frequencies->set(src_id, old_frequency);

                    // check whether this edge is already contained in the graph
                    if (dst_id < src_id) std::swap(src_id, dst_id);
                    edge_temporary.m_source = src_id;
                    edge_temporary.m_destination = dst_id;
                } while (edges_stored.count(edge_temporary) > 0); // and repeat...

                uint64_t edge_key = unif_uint64_t(m_random);
                assert(edge_key != 0 && "0 is reserved for the edges of the final graph");
                edges_stored[edge_temporary] = edge_key;
                temporary_edges.insert(edge_key, edge_temporary);
                output.emit(m_vertices[ edge_temporary.source() ], m_vertices[ edge_temporary.destination() ], 0.0);

//                COUT_DEBUG("INSERT_TEMP " << edge_temporary.source() << " -> " << edge_temporary.destination());
            };

        } else { // remove a temporary edge
            assert(!temporary_edges.empty() && "There are no temporary edges to remove");
            uint64_t random_key = unif_uint64_t(m_random);

            uint64_t edge_key;
            Edge edge_temporary;
            { // restrict the scope
                auto it = temporary_edges.iterator(random_key, std::numeric_limits<uint64_t>::max());
                if (it->has_next()) {
                    it->next(&edge_key, &edge_temporary);
                } else {
                    edge_key = temporary_edges.key_min();
                    assert(edge_key != 0 && "The value 0 is reserved for final edges");
                    temporary_edges.find(edge_key, &edge_temporary);
                }
            }
            assert(edges_stored.count(edge_temporary) > 0 && "Edge not present in the graph");
            assert(edges_stored[edge_temporary] == edge_key && "Key mismatch");

            // The (a,b)-tree may contain multiple edges with the same key (duplicates). In case we removed the
            // wrong edge, reinsert it with a new key
            Edge edge_removed;
            while (temporary_edges.remove(edge_key, &edge_removed) && edge_removed != edge_temporary) {
                uint64_t new_key = unif_uint64_t(m_random);
                temporary_edges.insert(new_key, edge_removed);
                edges_stored[edge_removed] = new_key;
            };

            edges_stored.erase(edge_temporary);
            output.emit(m_vertices[ edge_temporary.source() ], m_vertices[ edge_temporary.destination() ], -1.0);
        };

        num_ops_performed++;
    }

    assert(temporary_edges.empty() && "There are still temporary edges");
    assert(edges_final_position == m_num_edges_final && "Not all final edges have been inserted");
    assert(edges_stored.size() == m_num_edges_final &&
               "The hash table to keep track which edges are in the graph does not match the edges that "
               "should be present at the end of the generation process");
    assert(num_ops_performed == m_num_operations && "Generated a different number of operations than what requested");

    timer.stop();
    LOG("Operations generated in " << timer << ". Writing the final edges in the log file ... ");
}