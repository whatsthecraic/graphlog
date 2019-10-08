#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "edge.hpp"
#include "generator.hpp"

class Writer {
    static constexpr size_t NUM_OPERATIONS_PER_BLOCK = (1ull << 20);
//    static constexpr size_t NUM_OPERATIONS_PER_BLOCK = 16; // debug only
    using Property = std::pair<std::string, std::string>;
    std::vector<Property> m_properties;
    const uint64_t* m_vertices {nullptr};
    uint64_t m_vertices_sz {0};
    uint64_t m_num_vertices_final {0};
    const WeightedEdge* m_edges_final { nullptr };
    uint64_t m_edges_final_sz {0};
    const Generator::OpList* m_oplist { nullptr };
    uint64_t m_oplist_sz {0};
    void set_property0(const std::string& name, const std::string& value, bool internal = false);

    void write_metadata(std::fstream& file, uint64_t* out_marker_vertices_begin_temp, uint64_t* out_marker_edges_begin);
    void write_vertices(std::fstream &file, uint64_t start, uint64_t length);
    void write_sequence_operations(std::fstream& file);

    size_t edges_block_size(uint64_t num_operations = NUM_OPERATIONS_PER_BLOCK) const;

    void set_marker(std::fstream& file, std::streampos position);

public:
    // Initialise the instance
    Writer();

    // Destructor
    ~Writer();

    // Set a single property to store
    template<typename T>
    void set_property(const std::string& name, const T& value);

    // Set the vertices of the graph
    void set_vertices(const uint64_t* vertices, size_t vertices_sz, size_t num_vertices_final);

    // Set the sequence of final edges
    void set_edges(const WeightedEdge* edges, size_t edges_sz);

    // Set the sequence of operations
    void set_operations(const Generator::OpList* operations, size_t operations_sz);

    // Save the log in the given file
    void save(const std::string& path);
};


// Implementation details
template<typename T>
void Writer::set_property(const std::string &name, const T &value) {
    std::stringstream ss;
    ss << value;
    set_property0(name, ss.str());
}
