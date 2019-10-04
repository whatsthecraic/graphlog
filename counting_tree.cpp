#include "counting_tree.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>

#include "lib/common/error.hpp"

CountingTree::CountingTree(uint64_t num_entries, uint64_t index_node_size) : m_num_entries(num_entries), m_node_size(index_node_size) {
    if(index_node_size < 2) INVALID_ARGUMENT("Invalid block size: " << index_node_size);
    ::memset(m_subtree, 0, sizeof(m_subtree));

    if(m_num_entries > 0) {
        int height = ceil(log2(m_num_entries) / log2(m_node_size));
        if (height > m_max_height) { INVALID_ARGUMENT("Invalid number of keys/segments: too big"); }

        // we have B^h for the leaves (nodes at height =1), B^{H-1} for height =2, B^{H-2} for height =3, ...
        // compute it directly, to avoid overflows with the geometric series
        uint64_t tree_sz = 0;
        for(uint64_t i =0, B = m_node_size; i < height; i++, B *= m_node_size){
            tree_sz += B;
            m_subtree[i].m_phys_size = tree_sz;
        }
        int rc = posix_memalign((void**) &m_index, /* alignment */ 64,  /* size */ tree_sz * sizeof(value_t));
        if(rc != 0) { throw std::bad_alloc(); }
        ::memset((void*) m_index, 0, tree_sz * sizeof(value_t));
        m_height = height;

        // set the height of all the rightmost subtrees in the index
        while (height > 0) {
            uint64_t subtree_sz = pow(m_node_size, height - 1);
            uint64_t rightmost_subtree_sz = num_entries % subtree_sz;
            m_subtree[height - 1].m_rightmost_root_sz = (num_entries / subtree_sz) + (rightmost_subtree_sz != 0);
            assert(m_subtree[height - 1].m_rightmost_root_sz > 0);
            int rightmost_subtree_height = 0;
            if (rightmost_subtree_sz > 0) {
                rightmost_subtree_height = ceil(log2(rightmost_subtree_sz) / log2(m_node_size));
            }
            m_subtree[height - 1].m_rightmost_height = rightmost_subtree_height;

            // next subtree
            num_entries = rightmost_subtree_sz;
            height = rightmost_subtree_height;
        }
    }
}

CountingTree::~CountingTree() {
    free(m_index); m_index = nullptr;
}

template<CountingTree::UpdateType type>
CountingTree::value_t CountingTree::update_slot(value_t* __restrict slot, value_t value1){
    value_t value0 = *slot;
    switch(type){
    case UpdateType::SET:
        if(value1 < 0) INVALID_ARGUMENT("The given value is negative: " << value1);
        *slot = value1;
        return value1 - value0;
    case UpdateType::SET_IF_UNSET:
        if(value0 == 0){
            *slot = value1;
            return value1;
        } else {
            return 0;
        }
    case UpdateType::ADD:
        *slot += value1;
        return value1;
    case UpdateType::SUBTRACT:
        if(value0 < value1) INVALID_ARGUMENT("The new value is going to be negative. Operation: " << value0 << " - " << value1);
        *slot -= value1;
        return -value1;
    default:
        assert(0 && "This should be unreachable");
        ERROR("Invalid operation");
    }
}

template<CountingTree::UpdateType type>
CountingTree::value_t CountingTree::update_rec(value_t* __restrict index, uint64_t position, value_t value, int32_t height, bool is_rightmost){
    assert(height > 0 && "This is going to lead to an infinite recursion");
    if(height == 1){ // base case
        return update_slot<type>(index + position, value);
    }

    // traverse the tree down
    uint64_t subtree_sz = subtree_reg_num_slots(height -1);
    uint64_t subtree_num_elts = subtree_reg_num_elts(height -1);
    uint64_t subtree_id = position / subtree_num_elts;
    uint64_t root_sz = is_rightmost ? m_subtree[height -1].m_rightmost_root_sz : m_node_size;
    value_t* subtree_start = index + m_node_size + subtree_id * subtree_sz;
    uint64_t subtree_pos = position - subtree_id * subtree_num_elts;
    assert(root_sz > 0);
    bool is_subtree_rightmost = is_rightmost && (subtree_id == root_sz -1);
    int32_t subtree_height = is_subtree_rightmost ? m_subtree[height -1].m_rightmost_height : height -1;

    value_t diff = update_rec<type>(subtree_start, subtree_pos, value, subtree_height, is_subtree_rightmost);

    // now traverse the tree up
    index[subtree_id] += diff;
    return diff;
}

template<CountingTree::UpdateType type>
void CountingTree::update(uint64_t position, value_t value){
    if(position >= size()) INVALID_ARGUMENT("Invalid position: " << position << ". The total size of the index is: " << size());
    value_t diff = update_rec<type>(m_index, position, value, m_height, true);
    m_total_count += diff;
}

void CountingTree::set(uint64_t position, CountingTree::value_t value) {
    update<UpdateType::SET>(position, value);
}

void CountingTree::unset(uint64_t position){
    set(position, 0);
}

uint64_t CountingTree::search(value_t value) const {
    if(value >= m_total_count) INVALID_ARGUMENT("The given value is greater than the total in the counting tree. Total count: " << m_total_count << " <= searched value: " << value);

    // visit the index
    value_t* __restrict base = m_index;
    int64_t offset = 0;
    int height = m_height;
    bool is_rightmost = true; // this is thre rightmost subtree

    while(height > 0){
        uint64_t subtree_sz = height >=2 ? subtree_reg_num_slots(height - 1) : 1;
        uint64_t subtree_num_elts = subtree_reg_num_elts(height -1);
        uint64_t node_sz = (is_rightmost) ? m_subtree[height -1].m_rightmost_root_sz : m_node_size;
        assert(node_sz > 0);
        uint64_t subtree_id = 0;
        uint64_t cumulative_sum = 0;

        while(value >= cumulative_sum + base[subtree_id]){
            cumulative_sum += base[subtree_id];
            subtree_id++;

            while(base[subtree_id] == 0) subtree_id++;
        }
        assert(subtree_id < node_sz && "It doesn't comply with the invariant on the total count");

        is_rightmost = is_rightmost && (subtree_id == node_sz -1);

        // next iteration
        base += /* the root of the subtree */  m_node_size + subtree_id * subtree_sz;
        value -= cumulative_sum;
        offset += subtree_id * subtree_num_elts;
        height = is_rightmost ? m_subtree[height -1].m_rightmost_height : height -1;
    }

    return offset;
}

uint64_t CountingTree::subtree_reg_num_elts(int32_t height) const {
    assert(height >= 0 && height <= m_height);
    if(height == 0) return 1; // base case, leaf

    uint64_t num_elts = m_subtree[height -1].m_phys_size;
    if(height > 1){
        num_elts -= m_subtree[height -2].m_phys_size;
    }
    return num_elts;
}

uint64_t CountingTree::subtree_reg_num_slots(int32_t height) const {
    assert(height > 0 && height <= m_height);
    return m_subtree[height -1].m_phys_size;
}

uint64_t CountingTree::size() const {
    return m_num_entries;
}

CountingTree::value_t CountingTree::total_count() const {
    return m_total_count;
}

static void dump_tabs(std::ostream& out, size_t depth){
    using namespace std;

    auto flags = out.flags();
    out << std::setw((depth-1) * 2 + 5) << setfill(' ') << ' ';
    out.setf(flags);
}

void CountingTree::dump_index(std::ostream& out, value_t* root, uint64_t start_position, int height, bool is_rightmost) const {
    using namespace std;
    if(height <= 0) return; // empty tree

    int depth = m_height - height +1;
    int64_t root_sz = (is_rightmost) ? m_subtree[height - 1].m_rightmost_root_sz : m_node_size; // full
    uint64_t subtree_num_elts = subtree_reg_num_elts(height -1);

    // preamble
    auto flags = out.flags();
    if(depth > 1) out << ' ';
    out << setw((depth -1) * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << "offset: " << root - m_index << ", root size: " << root_sz << "\n";
    out.setf(flags);

    dump_tabs(out, depth);
    out << "entries: ";
    for(size_t i = 0; i < root_sz; i++){
        if(i > 0) out << ", ";
        out << i << " => k:" << start_position + i * subtree_num_elts << ", v:" << root[i];
    }
    out << "\n";

    if(height >= 2) { // recursively dump the children
        for(uint64_t i = 0; i < root_sz; i++){
            bool is_rightmost_subtree = is_rightmost && (i == root_sz - 1);
            value_t* subtree_start = root + m_node_size + i * subtree_reg_num_slots(height -1);
            uint64_t subtree_absolute_position = start_position + i * subtree_num_elts;
            int subtree_height = is_rightmost_subtree ? m_subtree[height -1].m_rightmost_height : height - 1;
            dump_index(out, subtree_start, subtree_absolute_position, subtree_height, is_rightmost_subtree);
        }
    }
}

void CountingTree::dump(std::ostream& out) const {
    // dump the index
    out << "[Index] node size: " << m_node_size << ", height: " << m_height << ", size: " << size() << ", total count: " << total_count() << "\n";
    dump_index(out, m_index, 0, m_height, true);
}
