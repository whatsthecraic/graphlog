#pragma once

#include <cstdint>
#include <random>
#include <string>
#include <unordered_map>

#include "abtree.hpp"
#include "counting_tree.hpp"
#include "edge.hpp"


class Generator {
    uint64_t m_num_operations; // total number of operations (insertions/deletions of edges) to create
    uint64_t m_num_max_edges; // max number of edges that can be stored in the graph
    const uint64_t m_seed; // random generator seed

    uint64_t* m_vertices = nullptr; // vertices ID
    uint64_t m_num_vertices_final = 0; // num vertices that actually belong to the final graph
    uint64_t m_num_vertices_temporary = 0; // num vertices that are temporary, it will need to be removed from the final graphs
    WeightedEdge* m_edges_final = nullptr; // list of vertices that belong to the final graph
    uint64_t m_num_edges_final = 0; // the length of the array `m_edges_final'
    CountingTree* m_frequencies = nullptr; // the frequency  associated to each vertex in the graph. Initially the frequency is the number of edges attached in the loaded graph.
    std::unordered_map<Edge, bool> m_edges_present; // edges present during the creation of the graph
    std::mt19937_64 m_random;

    void init_read_input_graph(void* ptr_frequencies, const std::string& path_input_graph, double ef_vertices);
    void init_temporary_vertices(void* ptr_map_frequencies, void* ptr_array_frequencies, double sf_frequency);
    void init_counting_tree(void* ptr_array_frequencies);
    void init_permute_edges_final();
public:
    // Constructor
    Generator(const std::string& path_input_graph, double sf_frequencies, double ef_vertices, double ef_edges, double aging_factor, uint64_t seed);

    // Destructor
    ~Generator();

    // Generate the operations in the graph
    enum class OpType { INSERT_TEMP_EDGE, REMOVE_TEMP_EDGE, INSERT_FINAL_EDGE };
    struct OpList { OpType m_type; uint32_t m_source_index; uint32_t m_destination_index; };
    std::unique_ptr<OpList[]> generate();

    // Total number of vertices that will appear in the final graph
    uint64_t num_final_vertices() const { return m_num_vertices_final; }

    // Total number of temporary vertices
    uint64_t num_temporary_vertices() const { return m_num_vertices_temporary; }

    // Retrieve the array of vertices
    // Internal pointer, do not deallocate it
    const uint64_t* vertices() const { return m_vertices; }

    // Retrieve the array of (final) edges
    // Internal pointer, do not deallocate it
    const WeightedEdge* edges() const { return m_edges_final; }

    // Total number of (final) edges in the graph
    uint64_t num_edges() const { return m_num_edges_final; }

    // Total number of vertices generated
    uint64_t num_vertices() const { return num_final_vertices() + num_temporary_vertices(); }

    // Total number of operations created
    uint64_t num_operations() const { return m_num_operations; }
};