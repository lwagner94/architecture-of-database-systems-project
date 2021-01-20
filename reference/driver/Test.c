#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>

#include "server.h"

typedef struct timeval timeval;

const int32_t MAX_SHORT_KEY = 0x7FFFFFFFL;
const int64_t MAX_INT_KEY =   0x7FFFFFFFFFFFFFFFLL;
int SEQUENCE = 1;
int UNIFORM = 2;

static void generateKey( KeyType type, int dist, int index, Key *key)
{
    switch (type)
    {
        case SHORT:
        	if( dist == SEQUENCE )
        		key->keyval.shortkey = index;
        	else if( dist == UNIFORM )
        		key->keyval.shortkey = ( rand() % (MAX_SHORT_KEY - 1)) + 1;
            break;
        case INT:
        	if( dist == SEQUENCE )
				key->keyval.intkey = index;
        	else if( dist == UNIFORM )
        		key->keyval.intkey = ((((int64_t)rand() << 32) | (int64_t)rand()) % (MAX_INT_KEY - 1)) + 1;
            break;
        case VARCHAR:
        	if( dist == SEQUENCE )
        		sprintf ( key->keyval.charkey, "XYZ_%d", index );
        	else if( dist == UNIFORM )
        	{
				int size=128;
				int i;
				int len=8;//rand() % (size - 1)+1;
				static const char text[] =  "abcdefghijklmnopqrstuvwxyz"
											"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
				for (i = 0; i < len; i++)
					key->keyval.charkey[i] = text[rand() % (sizeof(text) - 1)];
				key->keyval.charkey[i] = '\0'; //terminates the string with a NULL
        	}
        	break;
    }
}

static void generatePayload( int index, char* payload)
{
	sprintf ( payload, "%d", index);
}

int testPerformance(void)
{
	IdxState *ixstate = NULL;
	TxnState *txstate = NULL;
	int i=0, j=0, k=0;
	long currentIndex=0;
	timeval start, end;

	//configuration
	KeyType type = SHORT; //INT, VARCHAR
	int input = SEQUENCE; // UNIFORM
	int numKeys = 10000000;
	int numOpsPerTX=100;


	//test run
	printf("running default test cases for KeyType=%d, input=%d, numKeys=%d, numOpsPerTX=%d\n",type,input,numKeys,numOpsPerTX);

	create(type, "IX1");
	openIndex("IX1",&ixstate);
	printf("index IX created and opened\n");

	Record* record=(Record *) malloc (sizeof (Record));
	Key* key=(Key*) malloc (sizeof (Key));
	key->type=type;
	char payload[20];


	/*insert test*/
	gettimeofday(&start, 0);
	for( i=0; i<numKeys;i++ )
	{
		if( i%numOpsPerTX==0 )
			beginTransaction(&txstate);

		generateKey(type,input,i, key);
		generatePayload(i,payload);
		insertRecord(ixstate, txstate, key, payload);

		if((i+1)%numOpsPerTX==0)
			commitTransaction(txstate);
	}
	gettimeofday(&end, 0);
	double time = ((double)(end.tv_sec - start.tv_sec))*1000+((double)(end.tv_usec - start.tv_usec))/1000;
	printf("%i tuples inserted in %f ms\n",numKeys,time);


	/*get test*/
	gettimeofday(&start, 0);
	beginTransaction(&txstate);

	for( i=0; i<numKeys;i++ )
	{
		generateKey(type,input,i, key);
		record->key = *key;
		get(ixstate, txstate, record);
	}

	commitTransaction(txstate);
	gettimeofday(&end, 0);
	time = ((double)(end.tv_sec - start.tv_sec))*1000+((double)(end.tv_usec - start.tv_usec))/1000;
	printf("%i tuples queried in %f ms\n",numKeys,time);


	/*scan test*/
	gettimeofday(&start, 0);
	beginTransaction(&txstate);

	while(getNext(ixstate,txstate,record)!=DB_END)
		;//printf("scaned record key: %d with payload: %s\n",(int)record->key.keyval.shortkey,record->payload);

	commitTransaction(txstate);
	gettimeofday(&end, 0);
	time = ((double)(end.tv_sec - start.tv_sec))*1000+((double)(end.tv_usec - start.tv_usec))/1000;
	printf("%i tuples scanned in %f ms\n",numKeys,time);


	/*delete test*/
	gettimeofday(&start, 0);
	for( i=0; i<numKeys;i++ )
	{
		if( i%numOpsPerTX==0 )
			beginTransaction(&txstate);

		generateKey(type,input,i, key);
		record->key = *key;
		generatePayload(i,record->payload);
		deleteRecord(ixstate, txstate, record);

		if((i+1)%numOpsPerTX==0)
			commitTransaction(txstate);
	}
	gettimeofday(&end, 0);
	time = ((double)(end.tv_sec - start.tv_sec))*1000+((double)(end.tv_usec - start.tv_usec))/1000;
	printf("%i tuples deleted in %f ms\n",numKeys,time);

	drop("IX1");
	printf("index IX dropped\n");
}

int main(void) {
	testPerformance();
}
