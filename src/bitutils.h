//
// Created by lukas on 12.01.21.
//

#pragma once



inline std::pair<uint8_t, uint8_t> calculateNextTwoIndices(const uint8_t* data, uint32_t level_div_by_two) {
    uint8_t byte = *(data + level_div_by_two);

    uint8_t first_index = (byte >> 4);
    uint8_t second_index = byte & 0xF;

    return std::make_pair(first_index, second_index);
}

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


inline void varcharToByteArray(uint8_t* dest, const uint8_t* src) {
    // TODO: Feed this externally?
    size_t len = strnlen((char*) src, MAX_VARCHAR_LEN);
    size_t offset = MAX_VARCHAR_LEN - len;

    memcpy(dest + offset, src, len);
//    memset(dest, 0, offset);
}