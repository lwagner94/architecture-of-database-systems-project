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
 * L2List.h (fka PayloadPages.h)
 *
 *  Created on: 13.01.2009
 *      Author: Peter B. Volk, MBoehm
 */

#ifndef L2LIST_H_
#define L2LIST_H_

#include "IXStruct.h"

ErrCode getNextL2Item(IXState *state, uint32 tid, L1Item* item, char **payloadID);
ErrCode getFirstL2Item(IXState *state, uint32 tid, L1Item *item,char **payloadID);
ErrCode insertL2Item(IXState *state, uint32 tid, L1Item *partition, byte *payload, byte len);
ErrCode deleteL2Item(IXState *state, uint32 tid, L1Item *item, byte *payload, byte len);

L2Item* createL2Item2(TMP_L0Item *ix);
L2Item* createL2Item3(L0Item *ix);

int countL2Items(L0Item* ix, L1Item* item);
int countL2Items2(TMP_L0Item* ix, L1Item* item);

#endif
