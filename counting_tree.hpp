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

#include <cinttypes>
#include <iostream>
/**
 *
 * The class is not thread safe
 */
class CountingTree {
public:
    using value_t = int64_t;
private:

    CountingTree(const CountingTree& ct) = delete;
    CountingTree& operator=(const CountingTree& ct) = delete;
    constexpr static uint64_t m_max_height = 8; // we can index up to 256G values
    const uint64_t m_node_size = 64; // size of each block in the index
    const uint64_t m_num_entries; // the total number of entries indexed in the tree

    value_t m_total_count = 0; // the sum of all values in the array
    value_t* m_index = nullptr; // the actual content of the index
    int32_t m_height = 0; // the height of the index

    /**
     * Keep track of 1. the size of each subtree and 2. the cardinality and the height of the rightmost subtrees
     */
    struct SubtreeInfo {
        uint64_t m_phys_size; // The total number of slots used by a regular subtree
        uint32_t m_rightmost_root_sz; // The number of elements in the root of the rightmost subtree
        int32_t m_rightmost_height; // The height of the rightmost subtree
    };
    SubtreeInfo m_subtree[m_max_height];

    // Return the number of indexed entries for a __regular__ subtree at the given height
    uint64_t subtree_reg_num_elts(int32_t height) const;

    // Return the number of slots used by a __regular__ subtree at the given height
    uint64_t subtree_reg_num_slots(int32_t height) const;

    // Dump the given subtree in the index
    void dump_index(std::ostream& out, value_t* root, uint64_t start_position, int height, bool is_rightmost) const;

    // Set the value associated to a field
    enum class UpdateType { SET, SET_IF_UNSET, ADD, SUBTRACT };
    template<UpdateType type>
    value_t update_slot(value_t* __restrict slot, value_t new_value, value_t* out_old_value);
    template<UpdateType type>
    value_t update_rec(value_t* __restrict index, uint64_t position, value_t value, int32_t height, bool is_rightmost, value_t* out_old_value);
    template<UpdateType type>
    void update(uint64_t position, value_t value, value_t* out_old_value);

public:
    // Create a CountingTree with a given fixed size
    CountingTree(uint64_t num_entries, uint64_t index_node_size = 64);

    // Destructor
    ~CountingTree();

    // Set the score for the value at the given position
    void set(uint64_t position, value_t value, value_t* out_old_value = nullptr);

    // Reset to zero the score for the value at the given position
    void unset(uint64_t position, value_t* out_old_value = nullptr);

    // Return the first position such as the cumulative sum of all positions before is greater than the given value
    uint64_t search(value_t value) const;

    // Return the size of the tree (number of keys indexed)
    uint64_t size() const;

    value_t total_count() const;

    // Dump the content of the counting tree to the given output stream, for debugging purposes
    void dump(std::ostream& out = std::cout) const;
};