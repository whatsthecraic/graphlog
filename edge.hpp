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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <utility>

struct Edge {
    uint32_t m_source;
    uint32_t m_destination;

    Edge(): m_source{0}, m_destination{0} {}
    Edge(uint32_t source, uint32_t destination) : m_source(source), m_destination(destination) { };
    uint32_t source() const { return m_source; }
    uint32_t destination() const { return m_destination; }

    // Relative order of the edges
    bool operator==(const Edge&) const noexcept;
    bool operator!=(const Edge&) const noexcept;
    bool operator<(const Edge&) const noexcept;
    bool operator<=(const Edge&) const noexcept;
    bool operator>(const Edge&) const noexcept;
    bool operator>=(const Edge&) const noexcept;
};

struct WeightedEdge : public Edge {
    double m_weight;

    WeightedEdge();
    WeightedEdge(uint32_t src, uint32_t dst, double weight);
    double weight() const { return m_weight; }

    // Check whether two edges are equal
    bool operator==(const WeightedEdge&) const noexcept;
    bool operator!=(const WeightedEdge&) const noexcept;

    Edge edge() const;
};

std::ostream& operator<<(std::ostream& out, const Edge& e);
std::ostream& operator<<(std::ostream& out, const WeightedEdge& e);

namespace std {
template<>
struct hash<Edge> { // hash function for graph::Edge
private:
    // Adapted from the General Purpose Hash Function Algorithms Library
    // Author: Arash Partow - 2002
    // URL: http://www.partow.net
    // URL: http://www.partow.net/programming/hashfunctions/index.html
    // MIT License
    unsigned int APHash(uint64_t value) const {
        unsigned int hash = 0xAAAAAAAA;
        unsigned int i = 0;
        char* str = (char*) &value;

        for (i = 0; i < sizeof(value); ++str, ++i) {
            hash ^= ((i & 1) == 0) ? ((hash << 7) ^ (*str) * (hash >> 3)) :
                    (~((hash << 11) + ((*str) ^ (hash >> 5))));
        }

        return hash;
    }

public:
    size_t operator()(const Edge& e) const {
        return APHash((static_cast<uint64_t>(e.source()) << 32) + (e.destination() & 0xFFFFFFFF));
    }
};
} // namespace std
