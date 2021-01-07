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
 * IXServer.c
 *
 *  Created on: 16.12.2008
 *      Author: MBoehm
 */

//decomment for using huge pages
//#include <sys/ipc.h>
//#include <sys/shm.h>

// TODO: only copy key into state (on getNext) if new key was found (not everytimes) => only usefull for many duplicates
// TODO: different prefix length on different trie levels (e.g., 8, 8, 4, 4, 2, 2, 1, 1)
// TODO: only partial key comparisons at leaf nodes (depending on level)

#include "IX_Byte.h"
#include "IXStruct.h"
#include "TXManager.h"
#include "L2List.h"
#include "IndexDictionary.h"


//internal function declarations
static inline ErrCode internalGetNext(IXState* ixstate, uint32 tid, L1Item** item, char** l2Item, BOOL first);
static inline LogItem* createLogItem( L0Item* ix );
static inline LogItem* createLogItemList( L0Item* ix);

#if MULTIPLE_READERS == 1
static pthread_rwlock_t _L0_LOCK  = PTHREAD_RWLOCK_INITIALIZER;
#else
static pthread_mutex_t _L0_LOCK  = PTHREAD_MUTEX_INITIALIZER;
#endif

/**
 Creates a new index data structure to be used by any thread.

 @param type specifies what type of key the index will use
 @param name a unique name to be used to identify this index in any process
 @return ErrCode
 SUCCESS if successfully created index.
 DB_EXISTS if index with specified name already exists.
 FAILURE if could not create index for some other reason.
 */
ErrCode create(KeyType type, char *name)
{
	//enable huge pages support
	//shmget(IPC_PRIVATE, 1024*1024*1024, SHM_HUGETLB);

	//Part 1: optimistic create of new index
	L0Item* newIX = (L0Item *) MALLOC(SIZE_L0);
	//printf("Allocated Mem: %d\n",SIZE_L0);

	byte len = strlen(name);
	memcpy(newIX->name, (byte*)name, len+1); //not needed
	newIX->type = type;
	newIX->tx = NULL;

#if(PREALLOCATE_MEM>0)
	newIX->pos=1;
#endif

#if(REUSE_LOGITEMS==1)
	LogItem* log = createLogItemList(newIX);
	newIX->freeLog=_RPTR2(newIX,log);
#endif
#if(REUSE_IXLISTITEMS==1)
	newIX->freeIXLI=NULL;
#endif
#if(REUSE_TXLOGITEMS==1)
	newIX->freeTXLog=NULL;
#endif
#if(REUSE_L2ITEMS==1)
	newIX->freeL2=NULL;
#endif

	MUTEX_INIT(&(newIX->LOCK));

	initByteTrie(newIX);

	//Part 2: put index into dictionary

	//lock index dictionary
	MUTEX_WRITE_LOCK(&_L0_LOCK);

	//init index dictionary (create trie)
	initIndexDictionary();

	//insert newly created index
	ErrCode ret = internalInsertL0Item( newIX, len );

	//unlock index dictionary
	MUTEX_WRITE_UNLOCK(&_L0_LOCK);

	return ret;
}

ErrCode drop(char *name)
{
	//lock index dictionary
	MUTEX_WRITE_LOCK(&_L0_LOCK);

	//insert newly created index
	ErrCode ret = internalDeleteL0Item( (byte*)name );

	//unlock index dictionary
	MUTEX_WRITE_UNLOCK(&_L0_LOCK);

	return ret;
}

/**
 Opens a specific index data structure to be used by this thread.

 @param name the unique name specifying the index being opened
 @param idxState returns the state handle for the index being opened
 @return ErrCode
 SUCCESS if successfully opened index.
 DB_DNE if the name given does not have an associated DB that has been create()d.
 FAILURE if DB exists but could not be opened for some other reason.
 */
ErrCode openIndex(const char *name, IdxState **idxState)
{
	//find the specified index
	L0Item* ix = internalSearchL0Item(  (byte*)name );
	//printf("open index: %s\n",ix->name);

	if( ix )
	{
		IXState* ixstate = (IXState*) MALLOC( SIZE_IXSTATE );

		ixstate->ix           = ix;
		ixstate->keypos[0] 	  = '\0';
		ixstate->keylen       = 0;
		ixstate->key_notfound = FALSE;
		ixstate->posL2        = NULL;

		*idxState = (IdxState*) ixstate;

		return SUCCESS;
	}


	return DB_DNE;
}

/**
 Terminate use of current index by this thread.

 @param idxState The state variable for the index being closed
 @return ErrCode
 SUCCESS if succesfully closed index.
 DB_DNE is the DB never existed or was already closed by someone else.
 FAILURE if could not close DB for some other reason.
 **/
ErrCode closeIndex(IdxState *idxState)
{
	if ( idxState )
	{
		//free the memory
		FREE((IXState*) idxState);
		return SUCCESS;
	}

	return DB_DNE;
}


/**
 Signals the beginning of a transaction.  Each thread can have only
 one outstanding transaction running at a time.
 @param idxState The state variable for this thread
 @param type Specifies the type of transaction it will be.
 @return ErrCode
 SUCCESS if successfully began transaction.
 TXN_EXISTS if there is already a transaction begun for this thread.
 DEADLOCK if this transaction had to be aborted because of deadlock.
 FAILURE if could not begin transaction for some other reason.
 */
ErrCode beginTransaction(TxnState **txn)
{
	TXState *tx = (TXState *) MALLOC(SIZE_TXSTATE);
	tx->ix  = NULL;
	tx->tid = getInternalTID();

	*txn = (TxnState*) tx;

	return SUCCESS;
}


/**
 Forces the current transaction to abort, rolling back all changes
 made during the course of the transaction.

 @param idxState The state variable for this thread
 @return ErrCode
 SUCCESS if successfully aborted transaction.
 TXN_DNE if there was no transaction to abort.
 DEADLOCK if the abort failed because of deadlock.
 FAILURE if could not abort transaction for some other reason.
 */
ErrCode abortTransaction(TxnState *txn)
{
	TXState* txstate = (TXState*) txn;

	if ( txstate )
	{
		uint32 tid = txstate->tid;

		//foreach affected index - execute undo log
		L0Item* ix = NULL;

		//temp IX state
		IXState * ixstate;
		ixstate = (IXState *) MALLOC(SIZE_IXSTATE);
		ixstate->posL2=NULL;

		IXListItem* ixl = txstate->ix;

		while( ixl )
		{
			ix=ixl->ix;

			MUTEX_WRITE_LOCK(&(ix->LOCK));

			LogItem* log = abortTX( ix, tid );
			ixstate->ix=ix;

			/*execute log queue*/
			while (log)
			{
				LogItem* old = log;
				log = (LogItem*)_PTR(ix,log->next);

				byte* okey     = (byte*) _PTR(ix,old->key);
				byte* opayload = (byte*) _PTR(ix,old->payload);

#if(LEN_COMPRESSION_NUMERIC==1)
				byte len = old->len;
#else
				byte len;
				switch (ix->type)
				{
					case SHORT:		len = 4;					break;
					case INT:		len = 8;   					break;
					case VARCHAR:	len = strlen((char*)okey)+1;	break;
					default: return FAILURE;
				}
#endif

				byte lenp;
				if( opayload )
					lenp = strlen((char*)opayload);
				else
					lenp=0;

				/*rollback internalDelete*/
				if (old->del)
				{
					//execute internal insert
					L1Item* item = ixbInsertIntoTrie(ix, okey, len);
					insertL2Item(ixstate, 0, item, opayload, lenp); //tid=0->force
				}
				/*rollback internalInsert*/
				else
				{
					//execute internal delete
					ixbRemoveL1Item(ixstate, 0, okey, len, opayload, lenp); ////tid=0->force
				}
			}

			removeTX(ix, txstate->tid);
			txstate->ix=NULL;

			MUTEX_WRITE_UNLOCK(&(ix->LOCK));

			ixl = ixl->next;
		}

		//free temporary IXState
		FREE(ixstate);

		//free allocated memory of TXState
		FREE( txstate );

		return SUCCESS;
	}

	return TXN_DNE;;
}

/**
 Signals the end of the current transaction, committing
 all changes created in the transaction.

 @param idxState The state variable for this thread
 @return ErrCode
 SUCCESS if successfully ended transaction.
 TXN_DNE if there was no transaction currently open.
 DEADLOCK if this transaction could not be closed because of deadlock.
 FAILURE if could not end transaction for some other reason.
 */
ErrCode commitTransaction(TxnState *txn)
{
	TXState* txstate = (TXState*) txn;


	if ( txstate )
	{
		uint32 tid = txstate->tid;
		IXListItem* ixl = txstate->ix;
		L0Item* ix;

		while( ixl )
		{
			ix=ixl->ix;

			MUTEX_WRITE_LOCK(&(ix->LOCK));


			#if(BULK_LOAD==1)

				//printf("before commit -- count(ix)=%d \n",ixbCountDistinct(ix));
				ixbMergeTmpTrieIntoTrie(ix, 4); //TODO: variable key length
				ix->tmp_btrie=NULL;
				ix->tmp_nullPtr=NULL;
				//printf("after commit -- count(ix)=%d \n",ixbCountDistinct(ix));
			#endif

			removeTX( ix, tid );

#if( REUSE_IXLISTITEMS)
			IXListItem* old = ixl;

			ixl=ixl->next;

			old->next=ix->freeIXLI;
			ix->freeIXLI=old;

			MUTEX_WRITE_UNLOCK(&(ix->LOCK));
#else
			MUTEX_WRITE_UNLOCK(&(ix->LOCK));
			ixl=ixl->next;
#endif
		}

		//free allocated memory of TXState
		FREE( txstate );

		return SUCCESS;
	}

	return TXN_DNE;
}



/**
 Retrieve the first record associated with the given key value; if
 more than one record exists with this key, return the first record
 with this key.  Records with the same key may be returned in any
 order, but it must be that if there are n records with the same key
 k, a call to get followed by n-1 calls to getNext will return all n
 records with key k.

 @param idxState The state variable for this thread
 @param k Index key for the record to be retrieved.
 @param record Record into which the unique values are returned.
 @return ErrCode
 SUCCESS if successfully retrieved and returned unique record.
 KEY_NOTFOUND if specified key value was not found in the DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not retrieve unique record for some other reason.
 */
ErrCode get(IdxState *idxState, TxnState *txn, Record *record)
{
	ErrCode ret = KEY_NOTFOUND;
	IXState* ixstate = (IXState*) idxState;
	TXState* txstate = (TXState*) txn;
	L0Item* ix = ixstate->ix;
	uint32 tid=0;

	/*begin implicit TX*/
	if ( !txstate )
	{
		//internal auto commit
		tid = getInternalTID();
	}
	else
	{
		//external tx context
		tid = txstate->tid;
	}

	switch (ix->type) //no need for read lock (no memory allocated)
	{
		case SHORT:
			ixstate->keylen = 4;
			int32ToBA2(ixstate->keypos,record->key.keyval.shortkey);
			break;
		case INT:
			ixstate->keylen = 8;
			int64ToBA2(ixstate->keypos,record->key.keyval.intkey);
			break;
		case VARCHAR:
			ixstate->keylen = strlen( record->key.keyval.charkey)+1;
			memcpy(ixstate->keypos, (byte*)record->key.keyval.charkey,ixstate->keylen);
			break;
	}

	L1Item* item   = NULL;
	char*   l2Item = NULL;

	//lock ix
	MUTEX_READ_LOCK(&(ix->LOCK));

	//search L1 and L2 items
	if ( (item=ixbSearchL1Item(ix, ixstate->keypos, ixstate->keylen)) )
	{
		ret = getFirstL2Item(ixstate, tid, item, &l2Item);
	}

	//unlock ux
	MUTEX_READ_UNLOCK(&(ix->LOCK));

	if( ret==SUCCESS )
	{
		strcpy(record->payload, l2Item);
		ixstate->key_notfound = FALSE;
	}
	else //KEYNOTFOUND, DBEND_L2
	{
		ret = KEY_NOTFOUND;
		ixstate->key_notfound = TRUE;
		ixstate->posL2 = NULL;
	}

	return ret;
}

/**
 Retrieve the record following the previous record retrieved by get or
 getNext. If no such call has occurred since the current transaction
 began, or if this is called from outside of a transaction, this
 returns the first record in the index.

 @param idxState The state variable for the index whose next Record
 is to be returned
 @param record Record through which the next key/payload pair is returned
 @return ErrCode
 SUCCESS if successfully retrieved and returned the next record in the DB.
 DB_END if reached the end of the DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not retrieve next record for some other reason.
 */
ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
	ErrCode ret;
	IXState* ixstate = (IXState*) idxState;
	TXState* txstate = (TXState*) txn;
	L0Item* ix = ixstate->ix;
	uint32 tid;

	//begin implicit TX
	if (!txstate)
	{
		tid = getInternalTID();
	}
	else
	{
		tid = txstate->tid;
	}

	MUTEX_WRITE_LOCK(&(ix->LOCK));
	if( !isIX_TXaligned(ix, txstate) )
	{
		ixstate->keylen = 0;
		alignIX_TX(ix, txstate);
	}

	L1Item* item = NULL;
	char* l2Item = NULL;
	//PART 1: handle min record and keynotfound
	if ( ixstate->key_notfound || !ixstate->keylen )
	{
		if( !ixstate->key_notfound )
		{
			//search min record in trie
			item = ixbSearchMinL1Item(ix);
		}
		else
		{
			//search next key from key_notfound position

			//check for insert since last get
			if( !(item=ixbSearchL1Item(ix, ixstate->keypos, ixstate->keylen)) )
			{
				//get next from key_notfound position only if it still not exists
				item = ixbSearchNextL1Item(ix, ixstate->keypos, ixstate->keylen);
			}
		}

		if ( !item )
		{
			ret = DB_END;
		}
		else
		{
			ret = internalGetNext(ixstate,tid,&item,&l2Item, TRUE);
		}
	}
	//Part2: standard getNext
	else
	{
		//get Next record from current position
		ret = internalGetNext(ixstate,tid,&item,&l2Item, FALSE);
	}

	MUTEX_WRITE_UNLOCK(&(ix->LOCK));

	//build return value
	if( ret==SUCCESS )
	{
		if( item && item->key  ) //new l1item found during getNext
		{
			byte* ikey = (byte*)_PTR2(ix,item->key);

			//set key pos and record value
			switch (ix->type)
			{
				case SHORT:
					ixstate->keylen = 4;
					memcpy( ixstate->keypos, ikey, ixstate->keylen  );
					record->key.keyval.shortkey = baToInt32(ikey);
					break;
				case INT:
					ixstate->keylen = 8;
					memcpy( ixstate->keypos, ikey, ixstate->keylen  );
					record->key.keyval.intkey = baToInt64(ikey);
					break;
				case VARCHAR:
					ixstate->keylen = strlen((char*)ikey)+1;
					memcpy( ixstate->keypos, ikey, ixstate->keylen );
					memcpy(record->key.keyval.charkey, (char*) ikey, ixstate->keylen);
					break;
			}

		}
		else //using old key (get next L2 was successful)
		{
			//set record value
			switch (ix->type)
			{
				case SHORT:
					record->key.keyval.shortkey = baToInt32(ixstate->keypos);				  		break;
				case INT:
					record->key.keyval.intkey = baToInt64(ixstate->keypos);							break;
				case VARCHAR:
					memcpy(record->key.keyval.charkey, (char*) ixstate->keypos, ixstate->keylen); break;
			}
		}

		//set record payload
		strcpy(record->payload, l2Item );
		ixstate->key_notfound=FALSE;
	}
	//else
	//	printf(" return value: %d and key:%s\n ",ret,record->key.keyval.charkey);

	return ret;
}


static inline ErrCode internalGetNext(IXState* ixstate, uint32 tid, L1Item** l1Item, char** l2Item, BOOL first)
{
	ErrCode ret;
	L0Item* ix = ixstate->ix;

	if( first )
		ret = getFirstL2Item(ixstate, tid, *l1Item, l2Item);
	else
		ret = getNextL2Item(ixstate, tid, *l1Item, l2Item);

	if( ret == DB_END ) //DB_END used to determine end of single key as well
	{
#if(ABORT_GETNEXT_LOOPS != -1)
		short count=ABORT_GETNEXT_LOOPS;
		int oldL1=-1;
		int oldL2=-1;

#endif
		while (TRUE)
		{
#if( ABORT_GETNEXT_LOOPS!=-1 )
			if( !count-- )
			{
				printf("ABORT getnext loops: l1.key=:%d l2=%d ->break\n",(*l1Item)->key,(*l2Item));
				break;
			}
			else
			{
				if( oldL1==(int)(*l1Item) && oldL2==(int)(*l2Item) )
				{
					printf("ABORT getnext loops: same items detected ->break\n");
					break;
				}

				oldL1=(int)(*l1Item);
				oldL2=(int)(*l2Item);

			}
#endif

		    if( *l1Item )
		    {
		    	//using an intermediate found key
		    	byte llen;
		    	byte* lkey = (byte*)_PTR(ix,(*l1Item)->key);
				switch (ix->type)
				{
					case SHORT:   llen = 4;					break;
					case INT:     llen = 8;					break;
					case VARCHAR: llen = strlen( (char*)lkey )+1; 	break;
					default: return FAILURE;
				}

#if(ABORT_GETNEXT_CHECK==1)
				void* oldL1check=(*l1Item);

				*l1Item = ixbSearchNextL1Item(ix, lkey, llen);

				if( oldL1check==(*l1Item) )
		    	{
					//printf("search getNext returned old key for key len: %d\n",llen);
					return FAILURE;

		    	}
#else
				*l1Item = ixbSearchNextL1Item(ix, lkey, llen);
#endif
		    }
		    else
		    {
		    	//using the key given by ixstate
		    	*l1Item = ixbSearchNextL1Item(ix, ixstate->keypos, ixstate->keylen);
		    }


			if( !*l1Item )
			{
				return DB_END;
			}
			ret = getFirstL2Item(ixstate, tid, *l1Item ,l2Item);
			if(ret == SUCCESS)
			{
				return ret;
			}

		}
	}

	return ret;

}

/**
 Insert a payload associated with the given key. An identical key can
 be used multiple times, but only with unique payloads.  If this is
 called from outside of a transaction, it should commit immediately.

 @param idxState The state variable for this thread
 @param k key value for insert
 @param payload Pointer to the beginning of the payload string
 @return ErrCode
 SUCCESS if successfully inserted record into DB.
 ENTRY_EXISTS if identical record already exists in DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not insert entry for some other reason.
 */
ErrCode insertRecord(IdxState *idxState, TxnState *txn, Key *k, const char* payload)
{
	IXState* ixstate = (IXState*) idxState;
	TXState* txstate = (TXState*) txn;
	L0Item* ix = ixstate->ix;
	byte ilen, ilenp = strlen(payload);
	byte *ikey, *ipayload;
	uint32 tid = 0;

	//auto commit
	if ( txstate )
	{
		//external tx context
		tid = txstate->tid;
	}

	MUTEX_WRITE_LOCK(&(ix->LOCK));

	ALLOCATE_MEM(ipayload,ix,ilenp+1,byte);
	memcpy(ipayload, (byte*)payload, ilenp+1 );

	switch (ix->type)
	{
		case SHORT:
			ilen = 4;
			ikey = int32ToBA(k->keyval.shortkey,ix); //copy key
			break;
		case INT:
			ilen = 8;
			ikey = int64ToBA(k->keyval.intkey,ix); //copy key
			break;
		case VARCHAR:
			//printf("key=%s\n",k->keyval.charkey);
			ilen = strlen(k->keyval.charkey)+1;
			ikey =(byte*) charToBA(k->keyval.charkey, ilen,ix);
			break;
		default: return FAILURE;
	}


#if( BULK_LOAD==1 )
	ErrCode ret;
	//if(txstate)
	//{
		if( !isIX_TXaligned(ix, txstate) )
		{
			alignIX_TX(ix, txstate);
		}

		//execute internal insert
		L1Item* item = ixbInsertIntoTmpTrie(ix, ikey, ilen);
		ret = insertL2Item(ixstate, tid, item, ipayload, ilenp);
	//}
	//else //else branch for auto commit
	//{
	//	L1Item* item = ixbInsertIntoTrie(ix, ikey, ilen);
	//	ret = insertL2Item(ixstate, tid, item, ipayload, ilenp);
		//}
#else

	if( txstate )
	{
		//external TX context

		//create new log entry
		LogItem* newItem   = createLogItem( ix );
		newItem->del       = FALSE;
		newItem->next      = 0;

		newItem->payload = _RPTR(ix,ipayload);
		newItem->key     = _RPTR(ix,ikey);

		//append log entry to top
		logWriteOp(ix, txstate, newItem);
	}

	//execute internal insert
	L1Item* item = ixbInsertIntoTrie(ix, ikey, ilen);
	ErrCode ret = insertL2Item(ixstate, tid, item, ipayload, ilenp);
#endif
	//if( ret!=SUCCESS )
	//	printf("Error withret=%d\n",ret);

	MUTEX_WRITE_UNLOCK(&(ix->LOCK));

	return ret;
}

/**
 Remove the record associated with the given key from the index
 structure.  If a payload is specified in the Record, then the
 key/payload pair specified is removed. Otherwise, the payload pointer
 is NULL and all records with the given key are removed from the
 database.  If this is called from outside of a transaction, it should
 commit immediately.

 @param idxState The state variable for this thread
 @param record Record struct containing a Key and a char* payload
 (or NULL pointer) describing what is to be deleted
 @return ErrCode
 SUCCESS if successfully deleted record from DB.
 ENTRY_DNE if the specified key/payload pair could not be found in the DB.
 KEY_NOTFOUND if the specified key could not be found in the DB, with only the key specified.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not delete record for some other reason.
 */
ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record)
{
	IXState* ixstate = (IXState*) idxState;
	TXState* txstate = (TXState*) txn;
	L0Item* ix = ixstate->ix;
	byte ilen=0, ilenp = strlen(record->payload);
	byte *pkey, *ipayload=NULL;
	byte ikey[MAX_VARCHAR_LEN];
	uint32 tid=0; //force on 0 (no context)

	//begin implicit TX
	if (txstate)
	{
		tid = txstate->tid;
	}

	byte x=0;
	switch (ix->type)
	{
		case SHORT:
			ilen = 4;
			int32ToBA2(ikey,record->key.keyval.shortkey);
			//ikey = int32ToBA(record->key.keyval.shortkey,ix);
			break;
		case INT:
			ilen = 8;
			int64ToBA2(ikey,record->key.keyval.intkey);
			//ikey = int64ToBA(record->key.keyval.intkey,ix);
			break;
		case VARCHAR:
			ilen = strlen(record->key.keyval.charkey)+1;
			memcpy(ikey,(byte*)record->key.keyval.charkey,ilen);
			x=1;
			//ikey = charToBA(record->key.keyval.charkey,ilen+1,ix); //copy key for log
			break;
	}

	MUTEX_WRITE_LOCK(&(ix->LOCK));
	if( txstate && !isIX_TXaligned(ix,txstate) )
	{
		alignIX_TX(ix, txstate);
	}

	//execute internal delete
	ErrCode ret=ixbRemoveL1Item(ixstate, tid, ikey, ilen, (byte*)record->payload, ilenp);

	if( txstate && ret==SUCCESS)
	{
		//create new log entry
		LogItem* newItem = createLogItem( ix );
		newItem->del     = TRUE;
		newItem->next    = 0;

		//copy key
		ALLOCATE_MEM(pkey,ix,ilen+x,byte);
		memcpy(pkey,ikey,ilen+x);
		newItem->key     = _RPTR2(ix,pkey);

		//copy payload
		ALLOCATE_MEM(ipayload,ix,ilenp+1,byte)
		memcpy(ipayload, record->payload, ilenp+1);
		newItem->payload = _RPTR2(ix,ipayload);

		//append log entry to top
		logWriteOp(ix, txstate, newItem);
	}

	MUTEX_WRITE_UNLOCK(&(ix->LOCK));

	return ret;
}


//private functions

static inline LogItem* createLogItem( L0Item* ix )
{
	LogItem* item;

#if( REUSE_LOGITEMS )
	if(ix->freeLog)
	{
		item=(LogItem*)_PTR2(ix,ix->freeLog);
		ix->freeLog=item->next;
		return item;
	}
#endif

	ALLOCATE_MEM(item,ix,SIZE_LOGITEM,LogItem);
	return item;
}

static inline LogItem* createLogItemList( L0Item* ix )
{
	LogItem* first;
	ALLOCATE_MEM(first,ix,SIZE_LOGITEM,LogItem);
	LogItem* current = first;

	uint16 i;
	for( i=1; i<PREALLOCATE_LOGITEMS; i++  )
	{
		LogItem* tmp;
		ALLOCATE_MEM(tmp,ix,SIZE_LOGITEM,LogItem);

		current->next=_RPTR2(ix,tmp);
		current=tmp;
	}

	return first;
}

