/**
Copyright (c) 2009, TU Dresden - Database Technology Group
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of the Technische Universitaet Dresden nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/*
 * IX_Byte.h
 *
 * Header of IXByte.c (see the implementation for documentation)
 *
 * Created on: 13.01.2009
 *      Author: MBoehm
 */

#ifndef IX_BYTE_H_
#define IX_BYTE_H_
#include "IXStruct.h"

////////////////////////////////////////////////
// external function declarations (IX_Byte.c) //
////////////////////////////////////////////////

//create a new ixbyte trie structure
void initByteTrie(L0Item* ix);

//core ixbyte trie algorithms
L1Item* ixbSearchL1Item(L0Item *ix, byte* key, byte len);
L1Item* ixbSearchMinL1Item(L0Item *ix);
L1Item* ixbSearchNextL1Item(L0Item *ix, byte* okey, byte len);
ErrCode ixbRemoveL1Item(IXState* ixstate, uint32 tid, byte* key, byte len, byte *payload, byte lenp);
L1Item* ixbInsertIntoTrie(L0Item *ix, byte* key, byte len);


L1Item* ixbInsertIntoTmpTrie(L0Item* ix, byte* key, byte len);
void ixbMergeTmpTrieIntoTrie(L0Item* ix, byte len);

//operations
int ixbCountDistinct(L0Item *ix);
int ixbCountAll(L0Item *ix);

//allocate mem, convert and copy key
byte* int32ToBA(int32_t value, L0Item* ix);
byte* int64ToBA(int64_t value, L0Item* ix);
byte* charToBA(char* value, byte len, L0Item* ix);

int pmemcmp( byte* key1, byte* key2, uint16 level, uint16 max);

//convert and copy keys only
//inline void int32ToBA2(byte* key, int32_t value);
inline void int32ToBA2(byte* key, int32_t value)
{
 byte* dest = &key[3];
 *dest--= (byte)( value 	 & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
}
//inline void int64ToBA2(byte* key, int64_t value);

//reconvert and copy key only
//inline int32_t baToInt32(byte* value);
//inline int64_t baToInt64(byte* value);
inline void int64ToBA2(byte* key, int64_t value)
{
 byte* dest = &key[7];
 *dest--= (byte)( value 	 & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
 *dest--= (byte)((value>>=8) & 0xFF);
}


inline int32_t baToInt32(byte* value)
{
 int32_t data = 0;

 data |= (int32_t)value[0]<<24;
 data |= (int32_t)value[1]<<16;
 data |= (int32_t)value[2]<<8;
 data |= (int32_t)value[3];

 return data;
}

inline int64_t baToInt64(byte* value)
{
 int64_t data = 0;

 data |= (int64_t)value[0]<<56;
 data |= (int64_t)value[1]<<48;
 data |= (int64_t)value[2]<<40;
 data |= (int64_t)value[3]<<32;
 data |= (int64_t)value[4]<<24;
 data |= (int64_t)value[5]<<16;
 data |= (int64_t)value[6]<<8;
 data |= (int64_t)value[7];

 return data;
}


L1Item* createL1Item2(byte* key, byte len, TMP_L0Item* ix);
L1Item* createL1Item3(byte* key, byte len, L0Item* ix);
ixbTrieItem* createIxbTrieItem3(TMP_L0Item* ix);

#if( INSTRUMENTED_STATS==1 )
	double ixbGetMem(); //in MB
	double ixbGetMem2(L0Item* ix); //in MB
	void ixbResetMem();
#endif

//check, set and unset first bit of trie pointer (expansion flag)
#if(COMPRESS_PTR==1)

#define UNSET_EXPANDED(TRIE,POS) TRIE->ptr[POS] &= 0x7FFFFFFF;
#define SET_EXPANDED(TRIE,POS)   TRIE->ptr[POS] |= 0x80000000;
#define IS_EXPANDED(TRIE,POS)   (TRIE->ptr[POS] & 0x80000000)

#else

//#define UNSET_EXPANDED(TRIE,POS) (uint64)TRIE->ptr[POS] &= 0x7FFFFFFFFFFFFFFF;
//#define SET_EXPANDED(TRIE,POS)   (uint64)TRIE->ptr[POS] |= 0x8000000000000000;
//#define IS_EXPANDED(TRIE,POS)   ((uint64)TRIE->ptr[POS] & 0x8000000000000000)

#define UNSET_EXPANDED(TRIE,POS) (uint64)TRIE->ptr[POS] &= 0x7FFFFFFFFFFFFFFF;
#define SET_EXPANDED(TRIE,POS)   TRIE->ptr[POS]=(void*)(((uint64)TRIE->ptr[POS]) | 0x8000000000000000);
#define IS_EXPANDED(TRIE,POS)   ((uint64)TRIE->ptr[POS] & 0x8000000000000000)

#endif

//compute the slot index according to the choosen TI_N configuration
#if	(TI_N == 1)
	#define COMPUTE_INDEX(LEVEL,KEY,LEN) (( KEY[ (LEN-LEVEL)>>3 ]>>((LEVEL+1)%8)) & 0x1)
	#define COMPUTE_INDEX_LAST(KEY,LEN) ( KEY[ LEN-1 ] & 0x1 )
#elif (TI_N == 2)
	#define COMPUTE_INDEX(LEVEL,KEY,LEN) (( KEY[ (LEN-LEVEL)>>2 ]>>(((LEVEL+1)%4)<<1) ) & 0x3)
	#define COMPUTE_INDEX_LAST(KEY,LEN) ( KEY[ LEN-1 ] & 0x3 )
#elif (TI_N == 4)
	#define COMPUTE_INDEX(LEVEL,KEY,LEN) (( KEY[ (LEN-LEVEL)>>1 ]>>(((LEVEL+1)%2)<<2) ) & 0xF)
    #define COMPUTE_INDEX_LAST(KEY,LEN) ( KEY[ LEN-1 ] & 0xF )
#elif( TI_N == 8 ) // using one byte at once
	#define COMPUTE_INDEX(LEVEL,KEY,LEN) KEY[ LEN-(LEVEL) ]
    #define COMPUTE_INDEX_LAST(KEY,LEN) ( KEY[ LEN-1 ])
#endif

//bypass index top levels (high-len_level) in case of varchar keys
#if(TRIE_BYPASS==1)
 #define BYPASS_TOP_LEVELS(TRIE, LEVEL,LEN, IX){                             \
						if( IX->type == VARCHAR /*&& LEN!=TI_H_VARCHAR*/)	     \
						{													\
							TRIE = (ixbTrieItem*)_PTR2(IX,IX->jumper[LEVEL-LEN_LEVEL(LEN)]); \
							LEVEL=LEN_LEVEL(LEN);							\
						}													\
                    }

#else
 #define BYPASS_TOP_LEVELS(TRIE, LEVEL,LEN, IX)
#endif



#endif /* IX_BYTE_H_ */
