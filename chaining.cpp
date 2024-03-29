#include "readfile.h"

/* Hash function to choose bucket
 * Input: key used to calculate the hash
 * Output: HashValue;
 */
int modeHashCode(int key)
{
    return key % MBUCKETS;
}

int insertItem_chaining(int fd, DataItem item)
{
    int diff = 0;
    struct DataItem seacrhData;
    seacrhData.key = item.key;
    int Offset = searchItem_chaining(fd, &seacrhData, &diff, false); // search for this key as it may already exist
    if (Offset > 0)
    {
        item.nextOffset = seacrhData.nextOffset;
        int result_w = pwrite(fd, &item, sizeof(DataItem), Offset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
        return diff;
    }
    int hashIndex = modeHashCode(item.key);          //calculate the Bucket index
    int startingOffset = hashIndex * sizeof(Bucket); //calculate the starting address of the bucket
    Offset = startingOffset;                         //Offset variable which we will use to iterate on the db
    struct DataItem data;                            //a variable to read in it the records from the db
    struct DataItem prevData;                        //to hold data of current record
    int prevOffset;
    int count = 0;
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        ssize_t result_r = pread(fd, &data, sizeof(DataItem), Offset);
        //one record accessed
        count++;
        if (result_r <= 0 || data.valid == 0) //either an error happened in the pread or it hit an unallocated space
        {                                     // perror("some error occurred in pread");
            int result_w = pwrite(fd, &item, sizeof(DataItem), Offset);
            if (result_w < sizeof(DataItem))
            {
                perror("some error occurred in pwrite while inserting.");
            }
            return count;
        }
        else
        {
            prevOffset = Offset;
            Offset += sizeof(DataItem);
        }
    }

    prevData = data;
    int nextOffset = data.nextOffset;
    bool isEmpty = true;
    Offset = modeHashCode(MBUCKETS - 1) * BUCKETSIZE + BUCKETSIZE;
    while (true)
    { //loop to reach the end of the chain
        if (nextOffset == NULL)
            break;
        isEmpty = false;
        ssize_t result_r = pread(fd, &data, sizeof(DataItem), nextOffset);
        prevData = data;
        prevOffset = nextOffset;
        nextOffset = data.nextOffset;
        count++;
    }
    int overFlowStart = modeHashCode(MBUCKETS - 1) * BUCKETSIZE + BUCKETSIZE;
    int rewind = 0;
    if (!isEmpty)
        Offset = prevOffset + sizeof(DataItem);

    if (Offset >= FILESIZECHAINING)
    {
        Offset = overFlowStart;
        rewind = 1;
    }
RESEEK:
    //on the linux terminal use man pread to check the function manual
    ssize_t result = pread(fd, &data, sizeof(DataItem), Offset);
    //one record accessed
    count++;
    //check whether it is a valid record or not
    if (result <= 0 || data.valid == 0) //either an error happened in the pread or it hit an unallocated space
    {                                   // perror("some error occurred in pread");
        int result_w = pwrite(fd, &item, sizeof(DataItem), Offset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
        prevData.nextOffset = Offset;
        result_w = pwrite(fd, &prevData, sizeof(DataItem), prevOffset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
        return count;
    }
    else
    {
        Offset += sizeof(DataItem); //move the offset to next record
        if (Offset >= FILESIZECHAINING && rewind == 0)
        { //if reached end of the file start again
            rewind = 1;
            Offset = overFlowStart;
            goto RESEEK;
        }
        else if (rewind == 1 && Offset >= prevOffset)
        {
            printf("Over flow list is full.\n");
            return -2; //no empty spaces
        }
        goto RESEEK;
    }
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

int searchItem_chaining(int fd, struct DataItem *item, int *count, bool del)
{

    //Definitions
    struct DataItem data;                            //a variable to read in it the records from the db
    *count = 0;                                      //No of accessed records
    int rewind = 0;                                  //A flag to start searching from the first bucket
    int hashIndex = modeHashCode(item->key);         //calculate the Bucket index
    int startingOffset = hashIndex * sizeof(Bucket); //calculate the starting address of the bucket
    int Offset = startingOffset;                     //Offset variable which we will use to iterate on the db
    struct DataItem prevData;                        //to hold data of current record
    int prevOffset;
    for (int i = 0; i < RECORDSPERBUCKET; i++)
    {
        ssize_t result = pread(fd, &data, sizeof(DataItem), Offset);
        //one record accessed
        (*count)++;
        if (result <= 0) //either an error happened in the pread or it hit an unallocated space
        {                // perror("some error occurred in pread");
            return -1;
        }
        else if (data.valid == 1 && data.key == item->key)
        {
            //I found the needed record
            item->data = data.data;
            item->nextOffset = data.nextOffset;
            return Offset;
        }
        prevOffset = Offset;
        Offset += sizeof(DataItem);
    }
    prevData = data;
    int nextOffset = data.nextOffset;
    while (true)
    { //loop to reach the needed record of the chain
        if (nextOffset == NULL)
            break;
        ssize_t result_r = pread(fd, &data, sizeof(DataItem), nextOffset);
        (*count)++;
        if (data.valid == 1 && data.key == item->key)
        {
            //I found the needed record
            item->data = data.data;
            item->nextOffset = data.nextOffset;
            if (del && data.nextOffset == NULL)
            {
                prevData.nextOffset = NULL;
                ssize_t result_w = pwrite(fd, &prevData, sizeof(DataItem), prevOffset);
                if (result_w < sizeof(DataItem))
                {
                    perror("some error occurred in pwrite while inserting.");
                }
            }
            return nextOffset;
        }
        prevData = data;
        prevOffset = nextOffset;
        nextOffset = data.nextOffset;
    }
    // I didn't find the record
    return -1;
}

int deleteOffsetChaining(int fd, int Offset)
{
    struct DataItem dummyItem;
    struct DataItem data;
    dummyItem.valid = 0;
    dummyItem.key = -1;
    dummyItem.data = 0;
    ssize_t result_w;
    ssize_t result = pread(fd, &data, sizeof(DataItem), Offset); //one record accessed
    int nextOffset = data.nextOffset;
    if (nextOffset != NULL)
    {
        result = pread(fd, &data, sizeof(DataItem), nextOffset);
        result_w = pwrite(fd, &data, sizeof(DataItem), Offset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
        result_w = pwrite(fd, &dummyItem, sizeof(DataItem), nextOffset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
    }
    else
    {
        result_w = pwrite(fd, &dummyItem, sizeof(DataItem), Offset);
        if (result_w < sizeof(DataItem))
        {
            perror("some error occurred in pwrite while inserting.");
        }
    }
    return result_w;
}

int deleteChaining(int fd, int Offset, int key)
{
    struct DataItem data;
    struct DataItem data2;
    struct DataItem dummyItem;
    dummyItem.valid = 0;
    dummyItem.key = -1;
    dummyItem.data = 0;
    int hashIndex = modeHashCode(key);                                             //calculate the Bucket index
    int startingOffset = hashIndex * sizeof(Bucket);                               //calculate the starting address of the bucket
    int endingOffset = startingOffset + (RECORDSPERBUCKET - 1) * sizeof(DataItem); //calculate the address of record before the last one
    if (Offset >= startingOffset && Offset < endingOffset)
    {
        ssize_t result_r = pread(fd, &data, sizeof(DataItem), endingOffset); //read last record
        if (data.nextOffset != -1)
        {
            result_r = pread(fd, &data2, sizeof(DataItem), data.nextOffset);
            ssize_t result_w = pwrite(fd, &dummyItem, sizeof(DataItem), data.nextOffset);
            if (result_w < sizeof(DataItem))
            {
                perror("some error occurred in pwrite while inserting.");
            }
            data.nextOffset = data2.nextOffset;
            data2.nextOffset = NULL;
            result_w = pwrite(fd, &data, sizeof(DataItem), endingOffset);
            if (result_w < sizeof(DataItem))
            {
                perror("some error occurred in pwrite while inserting.");
            }
            result_w = pwrite(fd, &data2, sizeof(DataItem), Offset);
            if (result_w < sizeof(DataItem))
            {
                perror("some error occurred in pwrite while inserting.");
            }
            return result_w;
        }
    }
    return deleteOffsetChaining(fd, Offset);
}

/* Functionality: Display all the file contents
 *
 * Input:  fd: filehandler which contains the db
 *
 * Output: no. of non-empty records
 */
int DisplayFileChaining(int fd)
{

    struct DataItem data;
    int count = 0;
    int Offset = 0;
    for (Offset = 0; Offset < FILESIZE; Offset += sizeof(DataItem))
    {
        ssize_t result = pread(fd, &data, sizeof(DataItem), Offset);
        if (result < 0)
        {
            perror("some error occurred in pread");
            return -1;
        }
        else if (result == 0 || data.valid == 0)
        { //empty space found or end of file
            printf("Bucket: %d, Offset %d:~\n", Offset / BUCKETSIZE, Offset);
        }
        else
        {
            pread(fd, &data, sizeof(DataItem), Offset);
            printf("Bucket: %d, Offset: %d, Data: %d, key: %d\n", Offset / BUCKETSIZE, Offset, data.data, data.key);
            count++;
        }
    }

    printf("Overflow List --------------------\n");
    for (Offset = FILESIZE; Offset < FILESIZECHAINING; Offset += sizeof(DataItem))
    {
        ssize_t result = pread(fd, &data, sizeof(DataItem), Offset);
        if (result < 0)
        {
            perror("some error occurred in pread");
            return -1;
        }
        else if (result == 0 || data.valid == 0)
        { //empty space found or end of file
            printf("Offset %d:~\n", Offset);
        }
        else
        {
            pread(fd, &data, sizeof(DataItem), Offset);
            printf("Offset: %d, Data: %d, key: %d\n", Offset, data.data, data.key);
            count++;
        }
    }
    return count;
}