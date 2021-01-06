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
 * L2List.c
 *
 *  Created on: 25.02.2009
 *      Author: MBoehm
 */

#include "L2List.h"
#include "TXManager.h"


//local functions for L2List.c
static inline L2Item* createL2Item(L0Item *ix);


ErrCode getNextL2Item(IXState *state, uint32 tid, L1Item* item, char **payload)
{
	ErrCode ret = DB_END;
	L2Item* L2 = state->posL2;
	L0Item* ix = state->ix;

	while( L2 && !isReadable(ix,tid,L2->WTID) )
	{
		L2 = (L2Item*)_PTR(ix,L2->next);
	}

	if( L2 )
	{
		*payload = (char*)_PTR2(ix,state->posL2->payload);
		L2->RTID = tid;
		state->posL2 = (L2Item*)_PTR(ix,L2->next);
		return SUCCESS;
	}

	return ret;
}

ErrCode insertL2Item(IXState *state, uint32 tid, L1Item *partition, byte* payload, byte len)
{
	//NULL check
	if( !payload )
	{
		//do not insert this
		return SUCCESS;
	}

	L0Item* ix = state->ix;

	if( !partition->L2 ) //no existing L2
	{
		L2Item* L2 = createL2Item(ix);
		L2->payload = _RPTR2(ix,payload);
		L2->RTID    = tid;
		L2->WTID    = tid;
		L2->next    = 0;
		partition->L2 = _RPTR2(ix,L2);
		return SUCCESS;
	}
	else
	{
		L2Item* L2 = (L2Item*)_PTR2(ix,partition->L2); //at least one element

		while(TRUE)
		{

			if( memcmp( (byte*)_PTR2(ix,L2->payload), payload, len)==0 )
			{
				return ENTRY_EXISTS;
			}
			else if(!L2->next)
			{
				L2Item* tmp = createL2Item(ix);
				tmp->payload = _RPTR2(ix,payload);
				tmp->RTID    = tid;
				tmp->WTID    = tid;
				tmp->next    = 0;

				L2->next = _RPTR2(ix,tmp);
				return SUCCESS;
			}
			else
				L2 = (L2Item*)_PTR2(ix,L2->next);
		}
	}

	return FAILURE;
}





ErrCode deleteL2Item(IXState *state, uint32 tid, L1Item *item, byte *payload, byte len)
{
	BOOL force = (!tid)? TRUE : FALSE;
	L0Item* ix = state->ix;
	L2Item* L2 = (L2Item*)_PTR2(ix,item->L2);

	if( !force && !len )
	{
		while( L2 )
		{
			if( !isWritable(ix,tid,L2->WTID,L2->RTID) )
				return RETURN_INVISIBLE_L2;

#if( REUSE_L2ITEMS )
			L2Item* old = L2;
#endif
			L2=(L2Item*)_PTR(ix,L2->next);

#if( REUSE_L2ITEMS )
			old->next=_RPTR(ix,ix->freeL2);
			ix->freeL2=old;
#endif
		}

		item->L2=0;
		return SUCCESS;
	}

	//if payload is specified
	L2Item* last = NULL;
	while( L2 )
	{
		if( memcmp(payload,(char*)_PTR2(ix,L2->payload),len)==0  )
		{
			if( !force && !isWritable(ix,tid,L2->WTID,L2->RTID) )
			{
				//payload found but not allowed to delete
				return RETURN_INVISIBLE_L2;
			}
			else
			{
				//payload found and allowed to delete that
				if( !last )
					item->L2=L2->next;
				else
					last->next=L2->next;
				return SUCCESS;
			}
		}

		last = L2;
		L2 = (L2Item*)_PTR(ix,L2->next);
	}

	return SUCCESS; //payload not found (not included in the list)
}


ErrCode getFirstL2Item(IXState *state, uint32 tid, L1Item *item, char ** payload)
{
	ErrCode ret=DB_END;
	L0Item* ix = state->ix;
	L2Item* L2 = (L2Item*)_PTR2(ix,item->L2);

	if( L2 )
	{
		if( isReadable(ix,tid,L2->WTID))
		{
			*payload = (char*)_PTR2(ix,L2->payload);
			state->posL2 = (L2Item*)_PTR(ix,L2->next);
			return SUCCESS;
		}
		else
		{
			state->posL2 = (L2Item*)_PTR(ix,L2->next);
			return getNextL2Item(state, tid, item, payload);
		}
	}

	return ret;
}

int countL2Items(L0Item* ix, L1Item* item)
{
	int num=0;
	L2Item* L2 = (L2Item*)_PTR2(ix,item->L2);

	while( L2 )
	{
		num++;
		L2 = (L2Item*)_PTR(ix,L2->next);
	}

	return num;
}

int countL2Items2(TMP_L0Item* ix, L1Item* item)
{
	int num=0;
	L2Item* L2 = (L2Item*)_PTR2(ix,item->L2);

	while( L2 )
	{
		num++;
		L2 = (L2Item*)_PTR(ix,L2->next);
	}

	return num;
}



static inline L2Item* createL2Item(L0Item *ix)
{
	L2Item* item;

#if( REUSE_L2ITEMS==1 )
	if(ix->freeL2)
	{
		item=ix->freeL2;
		ix->freeL2=(L2Item*)_PTR(ix,ix->freeL2->next);
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_L2,L2Item);
	return item;
}


inline L2Item* createL2Item2(TMP_L0Item *ix)
{
	L2Item* item;

#if( REUSE_L2ITEMS==1 )
	if(ix->freeL2)
	{
		item=ix->freeL2;
		ix->freeL2=(L2Item*)_PTR(ix,ix->freeL2->next);
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_L2,L2Item);
	return item;
}

inline L2Item* createL2Item3(L0Item *ix)
{
	L2Item* item;

#if( REUSE_L2ITEMS==1 )
	if(ix->freeL2)
	{
		item=ix->freeL2;
		ix->freeL2=(L2Item*)_PTR(ix,ix->freeL2->next);
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_L2,L2Item);
	return item;
}
