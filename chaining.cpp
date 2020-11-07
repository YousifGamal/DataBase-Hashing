#include "readfile.h"

/* Hash function to choose bucket
 * Input: key used to calculate the hash
 * Output: HashValue;
 */
int modeHashCode(int key){
   return key % MBUCKETS;
}


int insertItem_chaining(int fd,DataItem item){
    int hashIndex = modeHashCode(item.key);  				//calculate the Bucket index
    int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db
    struct DataItem data;   //a variable to read in it the records from the db
    int count = 0;
    for(int i=0; i<RECORDSPERBUCKET; i++){
        ssize_t result_r = pread(fd,&data,sizeof(DataItem), Offset);
        //one record accessed
        count++;
        if(result_r <= 0 || data.valid == 0) //either an error happened in the pread or it hit an unallocated space
        { 	 // perror("some error occurred in pread");
            int result_w = pwrite(fd,&item,sizeof(DataItem), Offset);
            if(result_w < sizeof(DataItem)){
                perror("some error occurred in pwrite while inserting.");
            }
            return count;
        }else{
            Offset +=sizeof(DataItem);
        }
    }

    int nextOffset = data.nextOffset;
    if(nextOffset == -1){
        nextOffset = modeHashCode(MBUCKETS - 1) * BUCKETSIZE +  BUCKETSIZE 
                + hashIndex * OVERFLOWLISTSIZE*sizeof(DataItem); // Calculate next offset
        data.nextOffset = nextOffset; // assign next offset to last bucket record
        int result_w = pwrite(fd,&data,sizeof(DataItem), Offset - sizeof(DataItem));
        if(result_w < sizeof(DataItem)){
            perror("some error occurred in pwrite while inserting.");
        }
    }

    int overFlowListStartOffset = nextOffset;
    int overFlowListEndOffset = overFlowListStartOffset + OVERFLOWLISTSIZE*sizeof(DataItem);
    Offset = overFlowListStartOffset;
    struct DataItem prevdata;
    while (Offset < overFlowListEndOffset){
        ssize_t result_r = pread(fd,&data,sizeof(DataItem), Offset);
        //one record accessed
        count++;
        if(result_r <= 0 || data.valid == 0) //either an error happened in the pread or it hit an unallocated space
        { 	 // perror("some error occurred in pread");
            int result_w = pwrite(fd,&item,sizeof(DataItem), Offset);
            if(result_w < sizeof(DataItem)){
                perror("some error occurred in pwrite while inserting.");
            }
            return count;
        }else{
            Offset +=sizeof(DataItem);
        }
    }

    perror("Over flow list is full.");

    return count;
}



/* Functionality: using a key, it searches for the data item
 *          1. use the hash function to determine which bucket to search into
 *          2. search for the element starting from this bucket and till it find it.
 *          3. return the number of records accessed (searched)
 *
 * Input:  fd: filehandler which contains the db
 *         item: the dataitem which contains the key you will search for
 *               the dataitem is modified with the data when found
 *         count: No. of record searched
 *
 * Output: the in the file where we found the item
 */

int searchItem_chaining(int fd,struct DataItem* item,int *count)
{

	//Definitions
	struct DataItem data;   //a variable to read in it the records from the db
	*count = 0;				//No of accessed records
	int rewind = 0;			//A flag to start searching from the first bucket
	int hashIndex = modeHashCode(item->key);  				//calculate the Bucket index
	int startingOffset = hashIndex*sizeof(Bucket);		//calculate the starting address of the bucket
	int Offset = startingOffset;						//Offset variable which we will use to iterate on the db

	for(int i=0; i<RECORDSPERBUCKET; i++){
        ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);
        //one record accessed
        (*count)++;
        if(result <= 0) //either an error happened in the pread or it hit an unallocated space
        { 	 // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 1 && data.key == item->key) {
            //I found the needed record
                    item->data = data.data ;
                    return Offset;
        }
        Offset +=sizeof(DataItem);
    }

    int nextOffset = data.nextOffset;
    if(nextOffset == -1){
        return -1;
    }

    int overFlowListStartOffset = nextOffset;
    int overFlowListEndOffset = overFlowListStartOffset + OVERFLOWLISTSIZE*sizeof(DataItem);
    Offset = overFlowListStartOffset;
    while (Offset < overFlowListEndOffset){
        ssize_t result = pread(fd,&data,sizeof(DataItem), Offset);
        //one record accessed
        (*count)++;
        if(result <= 0) //either an error happened in the pread or it hit an unallocated space
        { 	 // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 1 && data.key == item->key) {
            //I found the needed record
                    item->data = data.data ;
                    return Offset;
        }
        Offset +=sizeof(DataItem);
    }

    return -1;
}

