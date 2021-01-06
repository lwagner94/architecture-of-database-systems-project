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
 * TXManager.h
 *
 *  Created on: 14.01.2009
 *      Author: MBoehm
 */

#ifndef TXMANAGER_H_
#define TXMANAGER_H_
#include "IXStruct.h"

//tx manager function

void logWriteOp( L0Item* ix, TXState* txstate, LogItem* item );

LogItem* abortTX( L0Item* ix, uint32 tid );
void removeTX( L0Item* ix, uint32 tid ); //removed BOOL commit

BOOL isReadable( L0Item* ix, uint32 currentTID, uint32 WTS );
BOOL isWritable( L0Item* ix, uint32 currentTID, uint32 WTS, uint32 RTS );

BOOL isIX_TXaligned( L0Item* ix, TXState* txstate );
void alignIX_TX( L0Item* ix, TXState* txstate );


//tid sequence with atomic increment
#ifdef __INTEL_COMPILER
	#define GET_NEXT_TID(VAR) _InterlockedIncrement(&VAR)
#else
	#define GET_NEXT_TID(VAR) __sync_fetch_and_add(&VAR,1)
#endif


uint32 getInternalTID();


#endif /* TXMANAGER_H_ */
