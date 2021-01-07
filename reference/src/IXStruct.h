
/**
Copyright (c) 2009, TU Dresden - Database Technology Group
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
    * Neither the name of the Technische Universitaet Dresden nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef IXSTRUCT_H
#define IXSTRUCT_H

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

#include "../server.h"

#define MAX_IX_NAME_LEN 16


//IXByte configuration
#define TI_N 4	        // n    4
#define TI_SIZE 16    // 2^n    16

#if( TI_N==2 )
	#define LEN_LEVEL(VAR) (VAR<<2)
#elif( TI_N==4 )
	#define LEN_LEVEL(VAR) (VAR<<1)
#elif( TI_N==8 )
	#define LEN_LEVEL(VAR) VAR
#else
    #define LEN_LEVEL(VAR) (VAR*(8/TI_N))
#endif

#if( TI_N==2 )
	#define LEN_KEY(VAR) (VAR>>2)
#elif( TI_N==4 )
	#define LEN_KEY(VAR) (VAR>>1)
#elif( TI_N==8 )
	#define LEN_KEY(VAR) VAR
#else
    #define LEN_KEY(VAR) (VAR*8/TI_N)
#endif

#define TI_H_INT LEN_LEVEL(8)    //8byte
#define TI_H_SHORT LEN_LEVEL(4)   //4byte
#define TI_H_VARCHAR LEN_LEVEL(MAX_VARCHAR_LEN) //128byte
#define TI_H_NAME LEN_LEVEL(MAX_IX_NAME_LEN) //16byte

//optimization flags
#define TRIE_BYPASS 0 //use jumper array to bypass the top level '0'-ptrs for keys of len<high (jump to high-len th level)
#define TRIE_PRE_ALLOCATION_JUMPER 0 //number of levels to preallocate on create varchar indices (from each '0'-ptr)
#define TRIE_PRE_ALLOCATION 0 //number of levels to preallocate on create numeric indices (from top level)
#define RETURN_INVISIBLE_L2 KEY_NOTFOUND //FAILURE
#define PREALLOCATE_MEM 4LL*(1024*1024*1024) //4GB
#define PREALLOCATE_MEM_ID 128*1024*1024 //Index Dictionary
#define COMPRESS_PTR 1 //concept of reduced pointers
#define PARTIAL_KEYCMP 0
#define REUSE_LOGITEMS 1   //reuse old logitems (only allocate if no free old one exist)
#define PREALLOCATE_LOGITEMS 200 // preallocate a list of reusable logitems
#define REUSE_IXLISTITEMS 1   //reuse old logitems (only allocate if no free old one exist)
#define REUSE_TXLOGITEMS 1   //reuse old logitems (only allocate if no free old one exist)
#define REUSE_L2ITEMS 0
#define ABORT_GETNEXT_LOOPS -1 //avoids endless loops; -1 to disable that
#define ABORT_GETNEXT_CHECK 1 //avoids endless loops; -1 to disable that
#define MULTIPLE_READERS 0 //turn this option on to allow concurrent readers on an index
#define DELETE_TRIENODES 0
#define INSTRUMENTED_STATS 0
#define DYNAMIC_PREFIX 0
#define BULK_LOAD 0 //TODO: so far not applicable by default because fixed data type + flushing during reading ops required

typedef struct L0Item L0Item;
typedef struct TMP_L0Item TMP_L0Item;
typedef struct ixbTrieItem ixbTrieItem;
typedef struct ixbTrieItem2 ixbTrieItem2;
typedef struct L1Item L1Item;
typedef struct TX_Log TX_Log;
typedef struct LogItem LogItem;
typedef struct IXState IXState;
typedef struct TXState TXState;
typedef struct L2Item L2Item;
typedef struct IXListItem IXListItem;
typedef unsigned char byte;
typedef unsigned short uint16;
typedef unsigned int uint32;
#if COMPRESS_PTR == 1
typedef unsigned int rptr32;
#else
typedef void* rptr32;
#endif
typedef unsigned long long uint64;

typedef enum
{ 	FALSE,
	TRUE
} BOOL;

/**
 * Alignment: no, see 'mem preallocation'
 */
struct L0Item
{
	TX_Log* tx;         // tx_log for all tx on that index
	KeyType type;
	ixbTrieItem* btrie;   //byte index
	L1Item* nullPtr; //nullPtr of byte index

//#if( BULK_LOAD==1 )
	ixbTrieItem* tmp_btrie;   //byte index
	L1Item* tmp_nullPtr; //nullPtr of byte index
//#endif

	rptr32* jumper; //bypass trie for huge h; reduced from: ixbTrieItem*

	byte name[ MAX_IX_NAME_LEN+1 ];

#if MULTIPLE_READERS == 1
	pthread_rwlock_t LOCK; //atomic access to single indices
#else
	pthread_mutex_t LOCK; //atomic access to single indices
#endif

#if(REUSE_LOGITEMS==1)
	rptr32 freeLog;
#endif
#if(REUSE_IXLISTITEMS==1)
	IXListItem* freeIXLI;
#endif
#if(REUSE_TXLOGITEMS==1)
	TX_Log* freeTXLog;
#endif
#if(REUSE_L2ITEMS==1)
	L2Item* freeL2;
#endif

#if( PREALLOCATE_MEM > 0 )
	byte mem[ PREALLOCATE_MEM ]; //preallocated memory

    #if(PREALLOCATE_MEM <= 0xFFFFFFFFL) //>4GB
	uint32 pos;
    #else
	uint64 pos;
	#endif
#endif
};

struct TMP_L0Item
{
	KeyType type;
	ixbTrieItem* btrie;   //byte index
	L1Item* nullPtr; //nullPtr of byte index
	//int count;

	rptr32* jumper; //bypass trie for huge h; reduced from: ixbTrieItem*

#if( PREALLOCATE_MEM > 0 )
	byte mem[ PREALLOCATE_MEM ]; //preallocated memory

	#if(PREALLOCATE_MEM <= 0xFFFFFFFFL) //>4GB
	uint32 pos;
	#else
	uint64 pos;
	#endif
#endif

};


/**
 * Alignment: TI_SIZE * 4
 *
 * First two bits are unset in all cases because at most 2^30
 * byte can be preallocated.
 */
struct ixbTrieItem
{
	//reduced pointers to nodes and leads
	//(1st bit used for expansion)
	rptr32 ptr[TI_SIZE]; //reduced from: int*
};

/**
 * Alignment: TI_SIZE * 8 byte
 */
struct ixbTrieItem2 //reduction possible with L0proxy-ptr, but slower
{
	int* ptr[TI_SIZE]; //generic pointers (int* for portability)
};

/**
 * Alignment: 8 byte
 */
struct L1Item //represents a unique key item
{
	rptr32 L2;     //reduced from: L2Item*
	rptr32 key;    //reduced from: byte*
};

/**
 * Alignment: 16 byte
 */
struct L2Item //represents a unique payload w.r.t. a specific key item
{
	rptr32 next; //reduced from: L2Item*
	uint32 RTID; //read timestamp
	uint32 WTID; //write timestamp
	rptr32 payload; //reduced from: char*
};

/**
 * Alignment: 16 byte
 */
struct TX_Log
{
	LogItem* log;    //not meaningful to reduce (16byte alignment)

	rptr32 next; //reduced from: TX_Log*
	uint32 tid;
};

/**
 * Alignment: 16 byte
 */
struct LogItem //tx/ix-local item of undo log
{
	rptr32 next;  //reduced from: LogItem*
	uint32 del; //true->delete;false->insert  //increased from: BOOL
	rptr32 key; //reduced from: byte*
	rptr32 payload; //reduced from: byte*
};


struct IXState
{
	L0Item* ix;
	L2Item* posL2;

	byte keypos[MAX_VARCHAR_LEN+1];
	byte keylen;

	BOOL key_notfound;
};

/**
 * Alignment: no
 */
struct TXState
{
	IXListItem* ix;
	uint32      tid;
};

/**
 * Alignment: 16 byte
 */
struct IXListItem
{
	IXListItem* next;
	L0Item* ix;
};


#define MALLOC(SIZE) malloc(SIZE)
#define FREE(VAR) free(VAR)

#if MULTIPLE_READERS == 0
	#define MUTEX_WRITE_LOCK(LOCK) pthread_mutex_lock(LOCK)
	#define MUTEX_READ_LOCK(LOCK) pthread_mutex_lock(LOCK)
	#define MUTEX_WRITE_UNLOCK(LOCK) pthread_mutex_unlock(LOCK)
	#define MUTEX_READ_UNLOCK(LOCK) pthread_mutex_unlock(LOCK)
	#define MUTEX_INIT(LOCK) pthread_mutex_init(LOCK, NULL)
#else
	#define MUTEX_WRITE_LOCK(LOCK) pthread_rwlock_wrlock(LOCK)
	#define MUTEX_READ_LOCK(LOCK) pthread_rwlock_rdlock(LOCK)
	#define MUTEX_WRITE_UNLOCK(LOCK) pthread_rwlock_unlock(LOCK)
	#define MUTEX_READ_UNLOCK(LOCK) pthread_rwlock_unlock(LOCK)
	#define MUTEX_INIT(LOCK) pthread_rwlock_init(LOCK, NULL)
#endif


#if( PREALLOCATE_MEM > 0 )

  #define SYNC_ALLOCATE_MEM(DEST,IX,LEN,CAST){			    \
			uint32 lpos= __sync_fetch_and_add(&IX->pos,LEN);\
			DEST = (CAST*)IX->mem[lpos];			\
    }

  #define ALLOCATE_MEM(DEST,IX,LEN,CAST){			    \
			DEST = (CAST*)&IX->mem[IX->pos];			\
			IX->pos+=LEN;								\
    }

#define ALLOCATE_MEM_ID(DEST,MEM,POS,LEN,CAST){			    \
			DEST = (CAST*)&MEM[POS];			\
			POS+=LEN;								\
  }

#define ALLOCATE_MEM2(DEST,IX,LEN,CAST){			    \
		if( (IX->pos+LEN)<PREALLOCATE_MEM )		\
		{												\
			DEST = (CAST*)&IX->mem[IX->pos];			\
			IX->pos+=LEN;								\
		}												\
		else											\
		{												\
			DEST=(CAST*) MALLOC(LEN);					\
		}												\
    }

	#if COMPRESS_PTR==1
		/*creates a pointer with given memory offset and nullcheck*/
		#define _PTR(IX, RPTR) ((RPTR)?(IX->mem+(uint32)RPTR):0)

		/*creates a pointer with given memory offset*/
		#define _PTR2(IX, RPTR) (IX->mem+(uint32)RPTR)
		#define _mPTR2(IX, RPTR) (IX->mem+(uint32)(RPTR&0x7FFFFFFF))

		/*creates a memory offset from a given pointer and nullcheck*/
		#define _RPTR(IX, PTR) (uint32)((PTR)?((byte*)PTR-IX->mem):0)

		/*creates a memory offset from a given pointer*/
		#define _RPTR2(IX, PTR) (uint32)((byte*)PTR-IX->mem)
	#else
		#define _PTR(IX, RPTR) RPTR
		#define _PTR2(IX, RPTR) RPTR
		#define _mPTR2(IX, RPTR) ((uint64)RPTR&0x7FFFFFFFFFFFFFFF)
		#define _RPTR(IX, PTR) PTR
		#define _RPTR2(IX, PTR) PTR
	#endif

#else
	#define ALLOCATE_MEM(DEST,IX,LEN,CAST) DEST=(CAST *) MALLOC(LEN);
#endif

//additional functionality
ErrCode drop(char *name);

#define SIZE_PTR sizeof(void*)
#if COMPRESS_PTR == 1
#define SIZE_COMPRESSED_PTR sizeof(uint32)
#else
#define SIZE_COMPRESSED_PTR sizeof(void*)
#endif
#define SIZE_TRIENODE sizeof(ixbTrieItem)
#define SIZE_TRIENODE2 sizeof(ixbTrieItem2)
#define SIZE_L0 sizeof(L0Item)
#define SIZE_TMPL0 sizeof(TMP_L0Item)
#define SIZE_L1 sizeof(L1Item)
#define SIZE_L2 sizeof(L2Item)
#define SIZE_TX_LOG sizeof(TX_Log)
#define SIZE_LOGITEM sizeof(LogItem)
#define SIZE_IXLIST sizeof(IXListItem)
#define SIZE_TXSTATE sizeof(TXState)
#define SIZE_IXSTATE sizeof(IXState)


#endif
