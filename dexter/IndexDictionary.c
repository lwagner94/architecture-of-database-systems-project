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
 * IndexDictionary.c
 *
 *  Created on: 03.05.2009
 *      Author: MBoehm
 */


#include "IndexDictionary.h"
#include "IX_Byte.h"

static ixbTrieItem2* _btrie = NULL;
static ixbTrieItem2* _jumper[TI_H_NAME];
static byte id_mem[ PREALLOCATE_MEM_ID ]; //preallocated memory
#if(PREALLOCATE_MEM_ID <= 0xFFFFFFFFL) //>4GB
static uint32 id_pos;
#else
static uint64 id_pos;
#endif


//static const byte _TI2_MASK[] = { 0x0, 0x2, 0x4, 0x6 };
//static const byte _TI4_MASK[] = { 0x0, 0x4 };

//private functions
static inline ixbTrieItem2* createIxbTrieItem2();

/**
 * Creates a new index dictionary using a modified IXByte structure.
 *
 * Unfortunately, no 'system index' with memory preallocation can be used
 * because the leaf nodes (L0Items) must be allocated by malloc and hence,
 * cannot be reproduced from the reduced pointers. An Additional L0Proxy
 * struct can solve this - however, it is nuch slower.
 *
 */
void initIndexDictionary( )
{
	if( !_btrie )
	{
		_btrie = createIxbTrieItem2();

		//create jumper array
		uint16 i, level=TI_H_NAME;
		ixbTrieItem2 *item = _btrie;
		_jumper[0]=item;
		for ( i=1; i<level ; i++ )
		{
			ixbTrieItem2* tmp = createIxbTrieItem2();
			item->ptr[0] = (int*)tmp;
			item = tmp;
			_jumper[i] = item;
		}

	}
}

/**
 * Searches the L0Item index dictionary by name. It returns the specified L0Item if it
 * exists, otherwise NULL.
 */
L0Item* internalSearchL0Item(byte* name)
{
	uint16 len = strlen((char*)name)+1;
	uint16 index, level = LEN_LEVEL(len);
	ixbTrieItem2* trie = _btrie;

	//jump to lower level
	trie = (ixbTrieItem2*) _jumper[TI_H_NAME-level];

	for( ; --level ; )
	{
		index = COMPUTE_INDEX2((level+1), name, len);

		if( !trie->ptr[index] )
		{
			return NULL; //index does not exist
		}

		trie = (ixbTrieItem2*)trie->ptr[index];
	}

	//level 1 = last trie level
	index = COMPUTE_INDEX_LAST2(name, len);
	return (L0Item*)trie->ptr[index]; //if null, index does not exist

}

/**
 * Inserts a newly created L0Item into the index dictionary. If another with same name
 * already exists it returns DB_EXISTS, otherwise the new index is inserted and SUCCESS
 * is returned.
 */
ErrCode internalInsertL0Item( L0Item* newIX, byte len )
{
	len +=1;
	uint16 index, level = LEN_LEVEL(len);
	byte* name = newIX->name;
	ixbTrieItem2* trie = _btrie;

	//jump to lower level
	trie = (ixbTrieItem2*) _jumper[TI_H_NAME-level];

	for( ; --level ; )
	{
		index = COMPUTE_INDEX2((level+1), name, len);

		if( trie->ptr[ index ])
		{
			trie = (ixbTrieItem2*)trie->ptr[ index ];
		}
		else
		{
			//create a new trie node
			ixbTrieItem2* tmp = createIxbTrieItem2();
			trie->ptr[ index ]= (int*) tmp;
			trie = tmp;
		}
	}

	//level 0 = leaf level
	index = COMPUTE_INDEX_LAST2(name,len);
	if( !trie->ptr[ index ]) //insert new index
	{
		trie->ptr[ index ] = (int*)newIX;
		return SUCCESS;
	}

	return DB_EXISTS;
}

ErrCode internalDeleteL0Item(byte* name)
{
	uint16 len = strlen((char*)name)+1;
	uint16 index, level = LEN_LEVEL(len);
	ixbTrieItem2* trie = _btrie;

	//jump to lower level
	trie = (ixbTrieItem2*) _jumper[TI_H_NAME-level];

	for( ; --level ; )
	{
		index = COMPUTE_INDEX2((level+1), name, len);

		if( !trie->ptr[index] )
		{
			return FAILURE; //index does not exist
		}

		trie = (ixbTrieItem2*)trie->ptr[index];
	}

	//level 1 = last trie level
	ErrCode ret;
	index = COMPUTE_INDEX_LAST2(name, len);
	if( trie->ptr[index] ) //delete if it exists
	{
		L0Item* tmp=(L0Item*)trie->ptr[index];
		FREE(tmp);
		trie->ptr[index]=NULL;
		ret = SUCCESS;
	}
	else
	{
		ret = FAILURE;
	}

	return ret;
}

ErrCode internalRenameL0Item(byte* name, byte* new_name)
{
	uint16 len = strlen((char*)name)+1;
	uint16 index, level = LEN_LEVEL(len);
	ixbTrieItem2* trie = _btrie;

	//jump to lower level
	trie = (ixbTrieItem2*) _jumper[TI_H_NAME-level];

	for( ; --level ; )
	{
		index = COMPUTE_INDEX2((level+1), name, len);

		if( !trie->ptr[index] )
		{
			return FAILURE; //index does not exist
		}

		trie = (ixbTrieItem2*)trie->ptr[index];
	}

	//level 1 = last trie level
	ErrCode ret;
	index = COMPUTE_INDEX_LAST2(name, len);
	if( trie->ptr[index] ) //delete if it exists
	{
		L0Item* tmp=(L0Item*)trie->ptr[index];

		memcpy(tmp->name, new_name, strlen(new_name)+1);
		ret = internalInsertL0Item( tmp, strlen(new_name) );

		trie->ptr[index]=NULL;
	}
	else
	{
		ret = FAILURE;
	}

	return ret;
}

/**
 * Allocates and initializes a new ixbTrieItem2 used as nodes within the
 * index dictionary trie.
 */
static inline ixbTrieItem2* createIxbTrieItem2()
{
	ixbTrieItem2* item;
	ALLOCATE_MEM_ID(item,id_mem,id_pos,SIZE_TRIENODE2,ixbTrieItem2);
	memset(item->ptr, 0, TI_SIZE*SIZE_PTR);

	return item;
}
