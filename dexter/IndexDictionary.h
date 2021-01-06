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
 * IndexDictionary.h
 *
 * Header-file of IndexDictionary.c - see the implementation file for documentation.
 *
 *
 *  Created on: 03.05.2009
 *      Author: MBoehm
 */

#ifndef INDEXDICTIONARY_H_
#define INDEXDICTIONARY_H_

#include "IXStruct.h"

//public functions
void initIndexDictionary();
L0Item* internalSearchL0Item(byte* name);
ErrCode internalInsertL0Item(L0Item* newIX, byte len);
ErrCode internalDeleteL0Item(byte* name);
ErrCode internalRenameL0Item(byte* name, byte* new_name);


#if	(TI_N == 1)
    #define COMPUTE_INDEX2(LEVEL,KEY,LEN) (( KEY[ LEN-( (LEVEL+1)>>3 ) ]>>((LEVEL+1)%8) ) & 0x1)
    #define COMPUTE_INDEX_LAST2(KEY,LEN) ( KEY[ LEN-1 ] & 0x1 )
#elif (TI_N == 2)
    #define COMPUTE_INDEX2(LEVEL,KEY,LEN) ( KEY[ LEN-( (LEVEL+1)>>2 ) ]>>( ((LEVEL+1)%4)<<1 ) ) & 0x3
    #define COMPUTE_INDEX_LAST2(KEY,LEN) ( KEY[ LEN-1 ] & 0x3 )
#elif (TI_N == 4)
	#define COMPUTE_INDEX2(LEVEL,KEY,LEN) ( KEY[ LEN-( (LEVEL+1)>>1 ) ]>>(((LEVEL+1)%2)<<2) ) & 0xF
    #define COMPUTE_INDEX_LAST2(KEY,LEN) ( KEY[ LEN-1 ] & 0xF )
#elif( TI_N == 8 ) // using one byte at once
	#define COMPUTE_INDEX2(LEVEL,KEY,LEN) KEY[ LEN-(LEVEL) ]
    #define COMPUTE_INDEX_LAST2(KEY,LEN) ( KEY[ LEN-1 ])
#endif


#endif  /* INDEXDICTIONARY_H_ */
