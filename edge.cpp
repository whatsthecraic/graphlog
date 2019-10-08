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