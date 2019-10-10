#pragma once

#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/FixedHashMap.h>

template <typename Key, typename TMapped>
struct FixedClearableHashMapCell
{
    using Mapped = TMapped;
    using State = ClearableHashSetState;

    using value_type = PairNoInit<Key, Mapped>;
    UInt32 version;
    Mapped mapped;

    FixedClearableHashMapCell() {}
    FixedClearableHashMapCell(const Key &, const State & state) : version(state.version) {}
    FixedClearableHashMapCell(const value_type & value_, const State & state) : version(state.version), mapped(value_.second) {}

    Mapped & getSecond() { return mapped; }
    const Mapped & getSecond() const { return mapped; }
    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }
};


template <typename Key, typename Mapped, typename Allocator = HashTableAllocator>
class FixedClearableHashMap : public FixedHashMap<Key, FixedClearableHashMapCell<Key, Mapped>, Allocator>
{
public:
    using key_type = Key;
    using mapped_type = Mapped;
    using value_type = typename FixedClearableHashMap::cell_type::value_type;

    mapped_type & operator[](Key x)
    {
        typename FixedClearableHashMap::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->second) mapped_type();

        return it->second;
    }

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
