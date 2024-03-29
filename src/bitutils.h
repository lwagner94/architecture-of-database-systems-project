//
// Created by lukas on 12.01.21.
//

#pragma once

inline void int32ToByteArray(uint8_t * data, int32_t number) {
    uint32_t temp = __builtin_bswap32(number);
    *reinterpret_cast<uint32_t*>(data) = temp;
}

inline void int64ToByteArray(uint8_t* data, int64_t number) {
    uint64_t temp = __builtin_bswap64(number);
    *reinterpret_cast<uint64_t*>(data) = temp;
}

inline int32_t charArrayToInt32(const uint8_t *data) {
    uint32_t temp = *reinterpret_cast<const uint32_t*>(data);
    return (int32_t) __builtin_bswap32(temp);
}

inline int64_t charArrayToInt64(const uint8_t *data) {
    uint64_t temp = *reinterpret_cast<const uint64_t*>(data);
    return (int64_t) __builtin_bswap64(temp);
}


inline size_t varcharToByteArray(uint8_t* dest, const uint8_t* src) {
    size_t len = strnlen((char*) src, MAX_VARCHAR_LEN);
    size_t offset = MAX_VARCHAR_LEN - len;

    memcpy(dest + offset, src, len);
    return offset;
}

inline std::pair<uint8_t, uint8_t> calculateNextTwoIndices(const uint8_t* data, uint32_t level_div_by_two) {
    uint8_t byte = *(data + level_div_by_two);

    uint8_t first_index = (byte >> 4);
    uint8_t second_index = byte & 0xF;

    return std::make_pair(first_index, second_index);
}

inline void prepareKeyData(const Key* k, uint8_t* dest) {
    switch (k->type) {
        case KeyType::SHORT:
            int32ToByteArray(dest, k->keyval.shortkey);
            break;
        case KeyType::INT:
            int64ToByteArray(dest, k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            varcharToByteArray(dest, (uint8_t *) k->keyval.charkey);
            break;
        default:
            return;
    }
}

inline uint32_t calculateIndex(const uint8_t* data, uint32_t level) {
    // Assuming prefix length = 4

    uint8_t byte = *(data + level / 2);
    uint8_t nibble = 0;

    if (level % 2 == 0) {
        nibble = (byte >> 4) & 0xF;
    }
    else {
        nibble = byte & 0xF;
    }

    return nibble;
}


