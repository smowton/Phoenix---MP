#ifndef _SERIALIZE_H
#define _SERIALIZE_H

#ifdef COMM_HACKS

// Default [de]serializers

template<typename V> void serialize_to(const V& val, std::ostream& o_str) {

  o_str.write((const char*)&val, sizeof(V));

}

template<typename V> void deserialize_from(V& result, std::istream& i_str) {

  i_str.read((char*)&result, sizeof(V));

}

#endif

#endif
