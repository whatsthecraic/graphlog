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

#include "edge.hpp"

#include <cassert>

using namespace std;

bool Edge::operator==(const Edge& e) const noexcept {
    return source() == e.source() && destination() == e.destination();
}
bool Edge::operator!=(const Edge& e) const noexcept{
    return !(*this == e);
}
bool Edge::operator<(const Edge& e) const noexcept {
    return (source() < e.source()) || (source() == e.source() && destination() < e.destination());
}
bool Edge::operator<=(const Edge& e) const noexcept {
    return (*this < e) || (*this == e);
}
bool Edge::operator>(const Edge& e) const noexcept {
    return !(*this <= e);
}
bool Edge::operator>=(const Edge& e) const noexcept {
    return !(*this < e);
}

WeightedEdge::WeightedEdge() : WeightedEdge(0,0,0){ }
WeightedEdge::WeightedEdge(uint32_t source, uint32_t destination, double weight) : Edge{source, destination}, m_weight(weight){
    assert(m_weight >= 0 && "Expected a non-negative value");
}

Edge WeightedEdge::edge() const {
    return Edge{source(), destination()};
}

bool WeightedEdge::operator==(const WeightedEdge& e) const noexcept {
    return source() == e.source() && destination() == e.destination() && weight() == e.weight();
}

bool WeightedEdge::operator!=(const WeightedEdge& e) const noexcept{
    return !(*this == e);
}

ostream& operator<<(std::ostream& out, const Edge& e) {
    out << "[src: " << e.source() << ", dst: " << e.destination() << "]";
    return out;
}

std::ostream& operator<<(std::ostream& out, const WeightedEdge& e){
    out << "[src: " << e.source() << ", dst: " << e.destination() << ", weight: " << e.weight() << "]";
    return out;
}