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
 * IX_Struct.h
 *
 *  Created on: 14.01.2009
 *      Author: MBoehm
 */


#include "TXManager.h"

/*
 * TXManager.c
 *
 *  Created on: 14.01.2009
 *      Author: MBoehm
 */


//global tid sequence
static int _tidSequence = 1;


/////////////////////
// local functions //
/////////////////////
static inline TX_Log* scanTXTable(L0Item* ix, uint32 tid);
static inline TX_Log* createTXLog( L0Item* ix );
static inline IXListItem* createIXListItem( L0Item* ix );


/**
 * Checks if the given index ix is already aligned to the given
 * txstate. This is required due to a N:M multiplicity between
 * indices and transactions.
 *
 * @return byte
 *   TRUE   if ix is already reference within the IXTable of txstate
 *   FALSE  otherwise
 */
BOOL isIX_TXaligned( L0Item* ix, TXState* txstate )
{
	IXListItem* ixl = txstate->ix;

	while( ixl )
	{
		if( ixl->ix == ix )
			return TRUE;

		ixl = ixl->next;
	}

	return FALSE;
}

void alignIX_TX( L0Item* ix, TXState* txstate)
{
	//1) Add TX to index-local tx table
	TX_Log* tx = createTXLog(ix);
	tx->tid  = txstate->tid;
	tx->log  = NULL;
	tx->next = _RPTR(ix,ix->tx);
	ix->tx   = tx;

	//2) Add IX to TX state
	IXListItem* ixl = createIXListItem(ix);
	ixl->ix=ix;
	ixl->next=txstate->ix;
	txstate->ix=ixl;

}

void logWriteOp( L0Item* ix, TXState* txstate, LogItem* item )
{
	TX_Log* tx = scanTXTable(ix, txstate->tid);

	if( !tx ) /*first write op on this ix during this tx*/
	{
		//1) Add TX to index-local tx table
		TX_Log* tx = createTXLog(ix);
		tx->tid  = txstate->tid;
		tx->log  = item;
		tx->next = _RPTR(ix,ix->tx);
		ix->tx   = tx;

		//2) Add IX to TX state
		IXListItem* ixl = createIXListItem(ix);
		ixl->ix=ix;
		ixl->next=txstate->ix;
		txstate->ix=ixl;
	}
	else
	{
		if (tx->log)
		{
			item->next = _RPTR2(ix,tx->log);
		}
		tx->log = item;
	}
}

LogItem* abortTX( L0Item* ix, uint32 tid )
{
	return scanTXTable( ix, tid )->log;
}

void removeTX( L0Item* ix, uint32 tid )
{
	TX_Log* tx;
	//check first item
	if( !ix->tx )
	{
		return;
	}
	else if( ix->tx->tid == tid )
	{
		tx=ix->tx;
		ix->tx=(TX_Log*)_PTR(ix,tx->next);
	}
	else
	{
		TX_Log* last = ix->tx;
		tx=(TX_Log*)_PTR(ix,ix->tx->next);

		while( tx && tx->tid!=tid )
		{
			last = tx;
			tx=(TX_Log*)_PTR(ix,tx->next);
		}

		if( tx )
		{
			//remove tx log from tx list
			last->next = tx->next;
		}
	}

#if( REUSE_TXLOGITEMS )
	tx->next=_RPTR(ix,ix->freeTXLog);
	ix->freeTXLog=tx;
#endif


#if( PREALLOCATE_MEM == 0 )
	//free log items
	LogItem* log = tx->log[index];
	LogItem* old = NULL;

	while (log != NULL)
	{
		old = log;
		log = log->next;

		if(    (commit && old->del==TRUE)      //commited delete
			|| (!commit && old->del==FALSE) )   //aborted insert
		{
			FREE(old->key);
			FREE(old->payload);
		}
#if(REUSE_LOGITEMS==0)
		FREE(old);
#endif
	}
#endif



#if( REUSE_LOGITEMS )
	LogItem* last = tx->log;
	if( last )
	{
		//scroll to last ptr of list
		while( last->next  )
		{
			last=(LogItem*)_PTR2(ix,last->next);
		}

		last->next=ix->freeLog;
		ix->freeLog = _RPTR2(ix,tx->log);
	}
#endif
}

/**
 * Currently used: Basic Timestamp Ordering
 **/
BOOL isReadable( L0Item* ix, uint32 currentTID, uint32 WTS )
{
	if(   (scanTXTable(ix, WTS) && currentTID!=WTS)  //isolation property
		||( currentTID < WTS)              )  		 //basic timestamp ordering
	{
		return FALSE;
	}

	return TRUE;
}

/**
 * Currently used: Basic Timestamp Ordering
 **/
BOOL isWritable( L0Item* ix, uint32 currentTID, uint32 WTS, uint32 RTS )
{
	if( currentTID < ( (WTS>RTS)? WTS : RTS ) )
	{
		return FALSE;
	}
	return TRUE;
}

static inline TX_Log* scanTXTable(L0Item* ix, uint32 tid)
{
	TX_Log* tx = ix->tx;

	//scan the list for tid
	while( tx && tx->tid!=tid )
	{
		tx=(TX_Log*)_PTR(ix,tx->next);
	}

	return tx;
}


inline uint32 getInternalTID()
{
	return GET_NEXT_TID(_tidSequence);
}


static inline TX_Log* createTXLog( L0Item* ix )
{
	TX_Log* item;

#if( REUSE_TXLOGITEMS )
	if(ix->freeTXLog)
	{
		item=ix->freeTXLog;
		ix->freeTXLog=(TX_Log*)_PTR(ix,ix->freeTXLog->next);
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_TX_LOG,TX_Log);

	return item;
}

//ixlist items
static inline IXListItem* createIXListItem( L0Item* ix )
{
	IXListItem* item;

#if( REUSE_IXLISTITEMS==1 )
	if(ix->freeIXLI)
	{
		item=ix->freeIXLI;
		ix->freeIXLI=ix->freeIXLI->next;
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_IXLIST,IXListItem);

	return item;
}
