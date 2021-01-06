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
 * IXByte.c
 *
 * Core Index Trie (Prefix Tree) for storing and retrieving
 * arbitrary byte[] keys. This is used in order to index
 * varchar, int32, int64 in a generic but efficient manner.
 *
 * The idea is to prevent any key comparisons by using
 * bit-sequences of the key to determine the path within
 * the tree. Thus, this index is a generalization of a
 * prefix tree (trie) for arbitrary data types.
 *
 * The difference between instances of different types is
 * the high of the trie:
 * - int32:   h = TI_H_SHORT   = 64/TI_N-1
 * - int64:   h = TI_H_INT     = 32/TI_N-1
 * - varchar: h = TI_H_VARCHAR = MAX_VARCHAR_LEN*8/TI_N-1
 *
 * Due to the huge high in case of type=varchar, we use
 * a so-called jumper array to jump to level h-k for a
 * certain key of length k.
 *
 * Furthermore, there are plenty of specific optimizations:
 * - trie expansion
 * - pre computation of all indices at once
 * - generic int pointers (for tree and leaf nodes)
 * - preallocation of memory
 * - alignment of ixbtrienodes with reduced pointers
 *
 *
 * Created on: 07.01.2009
 *      Author: MBoehm
 *
 */


#include "IX_Byte.h"
#include "L2List.h"
//#include <stdio.h>

/////////////////////////////////////////////////
// internal data variables (predefined masks) ///
/////////////////////////////////////////////////

//static const byte _TI2_MASK[] = { 0x0, 0x2, 0x4, 0x6 };
//static const byte _TI4_MASK[] = { 0x0, 0x4 };
static const uint16 _KEY_LEN[] = {TI_H_SHORT, TI_H_INT, TI_H_VARCHAR};


/////////////////////////////////////////////////
// internal function declarations  (IX_Byte.c) //
/////////////////////////////////////////////////
static L1Item* createL1Item(byte* key, byte len, L0Item* ix);
static ixbTrieItem* createIxbTrieItem(L0Item* ix);
static void ixbRPreallocateTrie(L0Item *ix, byte level, byte levels, ixbTrieItem* trie, BOOL ptr_aware);

#if( INSTRUMENTED_STATS==1 )
uint64 countNodes=0;

double ixbGetMem() //in MB
{
	return ((double)countNodes*SIZE_TRIENODE)/(1024*1024);
}

double ixbGetMem2(L0Item* ix) //in MB
{
	return ((double)ix->pos)/(1024*1024);
}

void ixbResetMem()
{
	countNodes=0;
}
#endif


void initByteTrie(L0Item *ix)
{
#if( INSTRUMENTED_STATS==1 )
	ixbResetMem();
#endif

	/*create byte trie index*/
	uint16 level = _KEY_LEN[ix->type];

	ix->btrie = createIxbTrieItem(ix);
	ix->nullPtr = NULL;

	//create jumper array
	if( ix->type==VARCHAR )
	{
#if(TRIE_BYPASS==1)
		ixbTrieItem *item = ix->btrie;
		ALLOCATE_MEM(ix->jumper,ix,level * SIZE_COMPRESSED_PTR,rptr32);
		ix->jumper[0]=_RPTR2(ix,item);

		uint16 i = level;
		for ( ; --i; )
		{

			ixbTrieItem* tmp = createIxbTrieItem(ix);
			item->ptr[0] = _RPTR2(ix,tmp);
			SET_EXPANDED(item,0);
#if(TRIE_PRE_ALLOCATION > 0)
			ixbRPreallocateTrie(ix, i, TRIE_PRE_ALLOCATION_JUMPER, tmp, TRUE);
#endif
			item = tmp;//item->ptr[0];
			ix->jumper[level - i] = _RPTR2(ix,item);
		}

		//preallocate first levels
#if(TRIE_PRE_ALLOCATION > 0)
		ixbRPreallocateTrie(ix, level, TRIE_PRE_ALLOCATION_JUMPER, ix->btrie, TRUE);
#endif
#endif
	}
#if(TRIE_PRE_ALLOCATION > 0)
	else
	{
		//preallocate first levels (with no awareness of jumperarrays)
		ixbRPreallocateTrie(ix, level, TRIE_PRE_ALLOCATION, ix->btrie, FALSE);
	}
#endif
}

/**
 * Retrieves an L1Item by given key (and key len).
 *
 * @return L1Item*
 *  A pointer to the specific L1Item that comprises all duplicates to that key.
 *  NULL in case of key not found.
 */
L1Item* ixbSearchL1Item(L0Item *ix, byte* key, byte len)
{
	//null check
	if( !len ) return ix->nullPtr;

	ixbTrieItem* trie = ix->btrie;
	uint16 index;
	uint16 level = _KEY_LEN[ix->type];
	uint16 max = _KEY_LEN[ix->type];

	//jump to level <trie_high-key_len> in case of varchar
	BYPASS_TOP_LEVELS(trie,level,len,ix);

	for( ; level-- ; )
	{
		index = COMPUTE_INDEX((level+1), key, max);

		if( !IS_EXPANDED(trie, index) )
		{
			//check if this is the right key
			if( !trie->ptr[index] )
				return NULL; //returns NULL if key does not exists

			L1Item* entry=(L1Item*)_PTR2(ix,trie->ptr[index]);
			byte* ekey = (byte*)(_PTR2(ix,entry->key));
			if(level) //if not at the leaf
				if( memcmp(ekey,key,len)==0 ) //check for right key
				{
					return entry;
				}
			else
				return entry;

			return NULL; //returns NULL if wrong key (with same prefix)
		}

		//go one level down
		trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[index]);
	}

	return NULL;
}

/**
 * Recursively retrieve the L1Item with the minimum key.
 *
 * @return L1Item
 * entry with minimum key
 * NULL if there is no entry in the index
 */
L1Item* ixbSearchMinL1Item(L0Item *ix)
{
	//null check
	if( ix->nullPtr ) return ix->nullPtr;

	//init top level vars
	ixbTrieItem *trie = ix->btrie;
	uint16 level=_KEY_LEN[ix->type];
	uint16 max = _KEY_LEN[ix->type];

	//init recursive state vars
	uint16 index[level];
	ixbTrieItem* parent[level];
	memset(index,0,level);
	memset(parent, 0, SIZE_PTR * level);

	//jump to level <trie_high-key_len> in case of varchar
	BYPASS_TOP_LEVELS(trie,level,1,ix);
	parent[level-2]=trie;

	uint16 i;
	BOOL down;
	while( TRUE )
	{
		down=FALSE;
		for( i=index[level-1]; i<TI_SIZE; i++ )
		{
			if ( !IS_EXPANDED(trie, i)) //search the minimum in local list
			{
				if( trie->ptr[i] )
				{
					return (L1Item*)_PTR2(ix,trie->ptr[i]);
				}
			}
			else
			if( trie->ptr[i] ) // go one level down
			{
				if( level==1 )
					return (L1Item*)_PTR2(ix,trie->ptr[i]);

				index[level-1]=i;
				parent[level-2]=trie;
				trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[ i ]);
				index[--level-1]=0;
				down=TRUE;
				break;
			}
		}

		//go one level up
		if( down )
			continue;
		else if( level<max )
		{
#if(TRIE_BYPASS==1)
			if( parent[level-1] )
				trie=(ixbTrieItem*)parent[level-1];
			else
				trie=(ixbTrieItem*)_mPTR2(ix,ix->jumper[max-level-1]);
#else
			trie=(ixbTrieItem*)parent[level-1];
#endif
			index[++level-1]+=1; //start from next position

		}
		else
			break;

	}

	return NULL;
}


/**
 * Recursively retrieve the next key after the given okey (with length len).
 *
 * Note that at top level this should be invoked with index2=NULL.
 */
L1Item* ixbSearchNextL1Item(L0Item *ix, byte* okey, byte len)
{
	L1Item* old = NULL;
	ixbTrieItem* trie = ix->btrie;
	uint16 level=_KEY_LEN[ix->type];
	uint16 max = _KEY_LEN[ix->type];

	uint16 index[level];
	memset(index, 0, level*2);

	ixbTrieItem* parent[ level ];
	memset(parent, 0, SIZE_PTR * level);

	//null check
	if( !len )
	{
		old = ix->nullPtr;
	}
	else
	{
		//jump to level <trie_high-key_len> in case of varchar
		BYPASS_TOP_LEVELS(trie,level,len,ix);
		parent[level-2]=trie;

		for( ; level; level-- ) //level>1
		{
			index[level-1] = COMPUTE_INDEX(level,okey,max);

			if( !IS_EXPANDED(trie, index[level-1]) )
			{
				//check if this is already a next key from current position
				if( trie->ptr[index[level-1]] )
				{
					L1Item* entry=(L1Item*)_PTR2(ix,trie->ptr[index[level-1]]);
					byte* ekey = (byte*)(_PTR2(ix,entry->key));
					if( memcmp(ekey,okey,len)>0 ) //check for right key if not at the leaf
					{
						return entry;
					}

				}

				//if no item or lower equal item, use it as old
				old = (L1Item*)_PTR2(ix,trie->ptr[ index[level-1] ]);
				index[level-1]+=1;
				break;
			}
			else if( trie->ptr[ index[level-1] ] )
			{
				parent[level-2]=trie;
				trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[ index[level-1] ]);
			}
			else
			{
				//Did not find right slot
				//printf("did not found old key -> break\n");
				old = NULL;
				break;
			}
		}


		//level 0 = leaf level
		/*if( !old ) //not found in unexpanded trie node
		{
			printf("FAILURE: did not found old key\n");

			index[level-1] = COMPUTE_INDEX_LAST(okey,max);

			if( trie->ptr[ index[level-1] ] ) //return existing entry
			{
				old = (L1Item*)_PTR2(ix,trie->ptr[ index[level-1] ]);
				index[level-1]+=1;

				printf("found old key at leaf node\n");
			}
		}*/
	}

	//try to search the next tupel from current position
	uint16 i;
	BOOL down;
	while( TRUE )
	{
		down = FALSE;
		//printf("index %d: %d \n",(level+1), index[level-1]);
		for( i=index[level-1]; i<TI_SIZE; i++ )
		{
			if ( !IS_EXPANDED(trie, i))
			{
				//trie not expanded (L1Items on that level)
				if( trie->ptr[i] )
				{
					return (L1Item*)_PTR2(ix,trie->ptr[i]);
				}
			}
			else
			if( level==1 )
			{
				if( trie->ptr[i] )
				{
					return (L1Item*)_PTR2(ix,trie->ptr[i]);
				}
			}
			else if( trie->ptr[i] ) // go one level down
			{
				index[level-1]=i;
				parent[level-2]=trie;
				trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[ i ]);
				index[--level-1]=0;
				down=TRUE;
				break;
			}
		}

		//go one level up
		if( down )
			continue;
		else if( level<max )
		{
#if(TRIE_BYPASS==1)
			if( parent[level-1] )
				trie=(ixbTrieItem*)parent[level-1];
			else
				trie=(ixbTrieItem*)_PTR2(ix,ix->jumper[max-level-1]);
#else
			trie=(ixbTrieItem*)parent[level-1];
#endif
			index[++level-1]+=1; //start from next position

		}
		else
			break;

	}

	//printf("return NULL in getNext\n");
	return NULL;
}


/**
	 * Recursively remove all Items accociated with the given key (with length len).
	 *
	 * Note that at top level this should be invoked with index2=NULL.
	 *
	 * @return int
	 * TRUE if all child are removed and hence, the parent can be removed as well.
	 * FALSE if other childs exists and hence, the parent must not be removed.
	 */
	ErrCode ixbRemoveL1Item(IXState* ixstate, uint32 tid, byte* key, byte len, byte *payload, byte lenp)
	{
		ErrCode ret = FAILURE;
		L0Item *ix = ixstate->ix;
		ixbTrieItem* trie = ix->btrie;
		uint16 index;
		uint16 level=_KEY_LEN[ix->type];
		uint16 max = _KEY_LEN[ix->type];

		//null check
		if( !len )
		{
			if( ix->nullPtr )
			{
				ret = deleteL2Item(ixstate, tid, ix->nullPtr, payload, lenp);

				if (!ix->nullPtr->L2)
				{
#if( PREALLOCATE_MEM == 0 )
					FREE(ix->nullPtr);
#endif
					ix->nullPtr = NULL;
				}
				return ret;
			}
			else
				return KEY_NOTFOUND;
		}

#if( DELETE_TRIENODES==1 )
		ixbTrieItem* parent[ level ];
		memset(parent, 0, SIZE_PTR * level);
		parent[level-2]=trie;
#endif

		//jump to level <trie_high-key_len> in case of varchar
		BYPASS_TOP_LEVELS(trie,level,len,ix);

#if( DELETE_TRIENODES==1 )
		parent[level-2]=trie;
#endif

		for( ; level-- ; )
		{
			index = COMPUTE_INDEX((level+1),key,max);

			if( !IS_EXPANDED(trie, index) )
			{
				break;
			}
			else
			if( trie->ptr[ index ] )
			{
#if( DELETE_TRIENODES==1 )
				parent[level-2]=trie;
#endif
				trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[ index ]);
			}
			else
			{
				return KEY_NOTFOUND;
			}
		}

		//level 0 = leaf level
		//if( !level )
		//	index = COMPUTE_INDEX_LAST(key,max);

		if( trie->ptr[ index ] )
		{
			//check if this is the right key
			L1Item* entry=(L1Item*)_PTR2(ix,trie->ptr[index]);
			byte* ekey = (byte*)(_PTR2(ix,entry->key));
			if( memcmp(ekey,key,len)!=0 ) //check for right key
			{
				return KEY_NOTFOUND;
			}

			ret = deleteL2Item(ixstate, tid, entry, payload, lenp);

			//remove trie pointers if necessary
			if (!entry->L2)
			{
#if( PREALLOCATE_MEM == 0 )
				FREE((L1Item*)trie->ptr[ index ]);
#endif
				trie->ptr[ index ]=0;
			}
			else
				return ret;
		}
		else
			return KEY_NOTFOUND;

#if( DELETE_TRIENODES==1 )
		byte i;
		while(TRUE)
		{
			for( i=0; i<TI_SIZE; i++ )
			{
				if( trie->ptr[i] )
				{
					//cannot delete this node, return
					return ret;
				}
			}

			if( ix->type==VARCHAR && _RPTR(ix,trie) == ix->jumper[max-level] )
			{
				//printf("preventing jump array from being deleted for %d\n",baToInt(key, len));
				break;
			}

			ixbTrieItem* old = trie;
			index = COMPUTE_INDEX(level+1,key,len);
			if( parent[level-1] )
				trie=(ixbTrieItem*)parent[level-1];
			else
				trie=(ixbTrieItem*)_PTR(ix,ix->jumper[max-level-1]);
			level++;
#if( PREALLOCATE_MEM == 0 )
			FREE((ixbTrieItem*)old);
#endif
			trie->ptr[index]=0;
			UNSET_EXPANDED(trie, index);
		}
#endif
		return ret;
	}

/**
 * Inserts a new distinct key into the index.
 * If key already exists it returns the existing L1Item (duplicate bucket)
 * otherwise, it returns a newly created L1Item for that key.
 *
 * @return L1Item for that key
 */
L1Item* ixbInsertIntoTrie(L0Item *ix, byte* key, byte len)
{
	//null check
	if( !len )
	{
		//printf("null check successful\n");
		if( !ix->nullPtr )
		{
			ix->nullPtr = createL1Item(key, len, ix);
		}
#if( PREALLOCATE_MEM == 0 )
		else
		{
			FREE(key); //store the key only once
			key=ix->nullPtr->key;
		}
#endif

		return ix->nullPtr;
	}

	ixbTrieItem *trie = ix->btrie;
	uint16 index;
	uint16 level = _KEY_LEN[ix->type];
	uint16 max = _KEY_LEN[ix->type];

	//jump to level <trie_high-key_len> in case of varchar
	BYPASS_TOP_LEVELS(trie,level,len,ix);

	for( ; level--; )
	{
		index = COMPUTE_INDEX((level+1),key,max);
		//printf("index %d: %d \n",(level+1), index);

		if( !IS_EXPANDED(trie, index) )
		{
			if( trie->ptr[ index ]  ) //return existing entry
			{
				L1Item* entry = (L1Item*)_PTR2(ix,trie->ptr[ index ]);
				rptr32 rentry =trie->ptr[ index ];


				byte* ekey = (byte*)(_PTR2(ix,entry->key));
				//printf("compare keys %s %s\n",ekey,key);
				if( memcmp(ekey,key,len)==0 )
				{
					return entry;
				}

				//printf("node splitting\n");
				//else splitting node
				ixbTrieItem* tmp;
				while(TRUE)
				{

					tmp = createIxbTrieItem(ix);

					trie->ptr[ index ]= _RPTR2(ix,tmp);
					SET_EXPANDED(trie, index);
					trie = tmp;
					level--;

					uint16 indexE1 = COMPUTE_INDEX((level+1),ekey,max);
					uint16 indexE2 = COMPUTE_INDEX((level+1),key,max);

					if( indexE1!=indexE2 )
					{
						trie->ptr[ indexE1 ] = rentry;
						L1Item* l1=createL1Item(key, len, ix);
						trie->ptr[ indexE2 ] = _RPTR2(ix,l1);
						return l1;
					}
					else
						index=indexE1;
				}
			}
			else //create new leaf L1 item
			{
				L1Item* l1=createL1Item(key, len, ix);
				trie->ptr[ index ] = _RPTR2(ix,l1);
				return l1;
			}
		}

		//go down one level
		trie = (ixbTrieItem*)_mPTR2(ix,trie->ptr[ index ]);

	}

	return NULL; //error occured
}




 /**
  * Converts a 4 byte int to byte (char*) sequence.
  * (Necessary due to little endian / big endian)
  *	X86 uses little endian, but the prefix tree requires
  *	a big endian scheme for being order preserving
  *
  *
  * Note that key is copied implicitely.
  * (use a simple cast where no copy is required)
  **/
 byte* int32ToBA(int32_t value, L0Item* ix)
 {
	 byte* data;
	 ALLOCATE_MEM(data,ix,4,byte);

	 byte* dest = &data[3];
	 *dest--= (byte)( value 	 & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);

	 return data;
 }

 /*inline void int32ToBA2(byte* key, int32_t value)
 {
	 byte* dest = &key[3];
	 *dest--= (byte)( value 	 & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
 }*/

 /**
  * Converts a 8 byte int to byte (char*) sequence.
  * (Necessary due to little endian / big endian)
  *
  * Note that key is copied implicitely.
  * (use a simple cast where no copy is required)
  **/
 byte* int64ToBA(int64_t value, L0Item* ix)
 {
	 byte* data;
	 ALLOCATE_MEM(data,ix,8, byte);

	 byte* dest = &data[7];
	 *dest--= (byte)( value 	 & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);
	 *dest--= (byte)((value>>=8) & 0xFF);

	 return data;
 }

 /*inline void int64ToBA2(byte* key, int64_t value)
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
 }*/

 /**
  * Converts a char array to byte (unsigned char) sequence.
  *
  * Note that key is copied implicitely.
  * (use a simple cast where no copy is required)
  */
 byte* charToBA(char* value, byte len, L0Item* ix)
 {
	byte* data;
	ALLOCATE_MEM(data, ix, len,byte);

	memcpy(data,value,len);

	return data;
 }

 L1Item* createL1Item2(byte* key, byte len, TMP_L0Item* ix)
 {

 	L1Item* item;
 	ALLOCATE_MEM(item,ix,SIZE_L1,L1Item);

 	item->key = _RPTR2(ix,key);
 	item->L2 = 0;

 	return item;
 }

 L1Item* createL1Item3(byte* key, byte len, L0Item* ix)
 {

 	L1Item* item;
 	ALLOCATE_MEM(item,ix,SIZE_L1,L1Item);

 	item->key = _RPTR2(ix,key);
 	item->L2 = 0;

 	return item;
 }

 ixbTrieItem* createIxbTrieItem3(TMP_L0Item* ix)
 {
#if( INSTRUMENTED_STATS==1 )
	 countNodes++;
#endif

 	ixbTrieItem* item;
 	ALLOCATE_MEM(item,ix,SIZE_TRIENODE,ixbTrieItem)

 	memset(item->ptr, 0, TI_SIZE*SIZE_COMPRESSED_PTR);

 	return item;
 }

 inline int pmemcmp( byte* key1, byte* key2, uint16 level, uint16 max) //TODO: ixbyte.h
 {
	uint16 offset = (max-level)/(8/TI_N);
	uint16 len = max/(8/TI_N);
	return memcmp(&key1[offset],&key2[offset],len-offset);
 }


 /////////////////////////////
 // private functions       //
 /////////////////////////////


 static void ixbRPreallocateTrie(L0Item *ix, byte level, byte levels, ixbTrieItem* trie, BOOL ptr_aware)
 {
 	if( levels<=0 || level<=1 )
 		return;

 	byte i=0+ptr_aware;
 	for( ; i<TI_SIZE; i++ ) //prevents jumper array from being deleted
 	{
 		ixbTrieItem* ixb = createIxbTrieItem(ix);
 		trie->ptr[i] = _RPTR2(ix,ixb);

 		SET_EXPANDED(trie,i);
 		ixbRPreallocateTrie(ix, level-1, levels-1, ixb, FALSE);
 	}
 }

 static L1Item* createL1Item(byte* key, byte len, L0Item* ix)
 {

 	L1Item* item;
 	ALLOCATE_MEM(item,ix,SIZE_L1,L1Item);

 	item->key = _RPTR2(ix,key);
 	item->L2 = 0;

 	return item;
 }



 static ixbTrieItem* createIxbTrieItem(L0Item* ix)
 {
#if( INSTRUMENTED_STATS==1 )
	 countNodes++;
#endif

 	ixbTrieItem* item;
 	ALLOCATE_MEM(item,ix,SIZE_TRIENODE,ixbTrieItem)

 	memset(item->ptr, 0, TI_SIZE*SIZE_COMPRESSED_PTR);

 	return item;
 }

