#include "readfile.h"
#include <math.h>
//calculate number of digits in given integer
int IntDigitCount(int x)
{
    int count = 0;
    while (x != 0)
    {
        x = x / 10;
        count++;
    }
    return count;
}
// Extract middle digit from given integer and its digit count
int getMiddleDigit(int x, int count)
{
    int location = (int)ceil(count / 2.0);
    return (x % (int)pow(10, location)) / (int)pow(10, location - 1);
}

int MeanSquareHashFunc(int key)
{
    int x = (int)pow(key, 2);
    int count = IntDigitCount(x);
    int bucketIndex = getMiddleDigit(x, count);
    return bucketIndex;
}

int modHashFunc(int key)
{
    return key % MBUCKETS;
}
//
int insertItem_multipleHashing(int fd, DataItem item)
{

    //Definitions
    struct DataItem data;                    //a variable to read in it the records from the db
    int count = 0;                           //No of accessed records
    int rewind = 0;                          //A flag to start searching from the first bucket
    int hashIndex = modHashFunc(item.key);   //calculate the Bucket index
    int Offset = hashIndex * sizeof(Bucket); //calculate the starting address of the bucket

    ssize_t result = 0;
    //Check Bucket from first hash
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        result = pread(fd, &data, sizeof(DataItem), Offset);
        count++;
        if (result <= 0) //either an error happened in the pread or it hit an unallocated space
        {                // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 0)
        {   // empty slot
            //Write the record
            int result = pwrite(fd, &item, sizeof(DataItem), Offset);
            return count;
        }
        Offset += sizeof(DataItem);
    }
    // if bucket from first hash is full
    //apply second hash

    //check if there is empty slot
    hashIndex = MeanSquareHashFunc(item.key);
    Offset = hashIndex * sizeof(Bucket);
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        result = pread(fd, &data, sizeof(DataItem), Offset);
        count++;
        if (result <= 0) //either an error happened in the pread or it hit an unallocated space
        {                // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 0)
        {   // empty slot
            //Write the record
            int result = pwrite(fd, &item, sizeof(DataItem), Offset);
            return count;
        }
        Offset += sizeof(DataItem);
    }
    // if still no place found use open addressing
    int startingOffset = Offset;

//Main Loop
MultiHashInsert:
    //Read slot
    result = pread(fd, &data, sizeof(DataItem), Offset);
    //one record accessed
    count++;
    //chech if slot is empty or not
    if (result <= 0) //either an error happened in the pread or it hit an unallocated space
    {                // perror("some error occurred in pread");
        return -1;
    }
    else if (data.valid == 0)
    {   // empty slot
        //Write the record
        int result = pwrite(fd, &item, sizeof(DataItem), Offset);
        return count;
    }
    else
    {                               //Slot was taken-> search for new empty slot
        Offset += sizeof(DataItem); //move the offset to next record
        if (Offset >= FILESIZE && rewind == 0)
        { //if reached end of the file start again
            rewind = 1;
            Offset = 0;
            goto MultiHashInsert;
        }
        else if (rewind == 1 && Offset >= startingOffset)
        {
            return -2; //no empty spaces
        }
        goto MultiHashInsert;
    }
}

int searchItem_multipleHashing(int fd, struct DataItem *item, int *count)
{

    //Definitions
    struct DataItem data;                    //a variable to read in it the records from the db
    *count = 0;                              //No of accessed records
    int rewind = 0;                          //A flag to start searching from the first bucket
    int hashIndex = modHashFunc(item->key);  //calculate the Bucket index
    int Offset = hashIndex * sizeof(Bucket); //calculate the starting address of the bucket

    ssize_t result = 0;
    //check bucket from first hash
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        result = pread(fd, &data, sizeof(DataItem), Offset);
        (*count)++;
        if (result <= 0) //either an error happened in the pread or it hit an unallocated space
        {                // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 1 && data.key == item->key)
        {
            //I found the needed record
            item->data = data.data;
            return Offset;
        }
        Offset += sizeof(DataItem);
    }
    // check bucket from 2nd hash
    hashIndex = MeanSquareHashFunc(item->key);
    Offset = hashIndex * sizeof(Bucket);
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        result = pread(fd, &data, sizeof(DataItem), Offset);
        (*count)++;
        if (result <= 0) //either an error happened in the pread or it hit an unallocated space
        {                // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 1 && data.key == item->key)
        {
            //I found the needed record
            item->data = data.data;
            return Offset;
        }
        Offset += sizeof(DataItem);
    }
    //Still not found
    // search using open addressing
    int startingOffset = Offset;

//Main Loop
MultiHashSearch:

    result = pread(fd, &data, sizeof(DataItem), Offset);

    (*count)++;
    //check whether it is a valid record or not
    if (result <= 0) //either an error happened in the pread or it hit an unallocated space
    {                // perror("some error occurred in pread");
        return -1;
    }
    else if (data.valid == 1 && data.key == item->key)
    {
        //I found the needed record
        item->data = data.data;
        return Offset;
    }
    else
    {                               //not the record I am looking for
        Offset += sizeof(DataItem); //move the offset to next record
        if (Offset >= FILESIZE && rewind == 0)
        { //if reached end of the file start again
            rewind = 1;
            Offset = 0;
            goto MultiHashSearch;
        }
        else if (rewind == 1 && Offset >= startingOffset)
        {
            return -1; //File not found
        }
        goto MultiHashSearch;
    }
}
