//============================================================================
// Name        : hashskeleton.cpp
// Author      :
// Version     :
// Copyright   : Code adapted From https://www.tutorialspoint.com/
// Description : Hashing using open addressing
//============================================================================

#include "readfile.h"
/*
#include "readfile.cpp"
#include "chaining.cpp"
#include "openAddressing.cpp"
*/

#define OPENADDRESSING 1
#define CHAINING 2
#define MULTIHASHING 3
#define TESTCASE1 1
#define TESTCASE2 2

int insert(int key, int data, int type);
int deleteItem(int key, int type);
struct DataItem *search(int key, int type);
int filehandle; //handler for the database file

// function to test  Addressing hashing
void testOpenAdd(int testCaseNum);

// function to test  Multiple hashing
void testMulHashing(int testCaseNum);

// function to test chaining hashing
void testChaining(int testCaseNum);

/* DBMS (DataBase Management System) needs to store its data in something non-volatile
 * so it stores its data into files (manteqy :)).

 * Some DBMS or even file-systems constraints the size of that file. 

 * for the efficiency of storing and retrieval, DBMS uses hashing 
 * as hashing make it very easy to store and retrieve, it introduces 
 * another type of problem which is handling conflicts when two items
 * have the same hash index

 * In this exercise we will store our database into a file and experience
 * how to retrieve, store, update and delete the data into this file 

 * This file has a constant capacity and uses external-hashing to store records,
 * however, it suffers from hashing conflicts.
 * 
 * You are required to help us resolve the hashing conflicts 

 * For simplification, consider the database has only one table 
 * which has two columns key and data (both are int)

 * Functions in this file are just wrapper functions, the actual functions are in openAddressing.cpp

*/

int main()
{

   int type;

   do
   {
      printf("Choose a Hashing Function\n\n");
      printf("1. Open Addressing\n");
      printf("2. Chaining\n");
      printf("3. MultipleHashing\n");
      printf("4. Exit\n");
      scanf("%d", &type);

      switch (type)
      {
      case 1:
         printf("Choose TestCase1 or TestCase2 :\n");
         scanf("%d", &type);
         testOpenAdd(type);
         break;
      case 2:
         printf("Choose TestCase1 or TestCase2 :\n");
         scanf("%d", &type);
         testChaining(type);
         break;

      case 3:
         printf("Choose TestCase1 or TestCase2 :\n");
         scanf("%d", &type);
         testMulHashing(type);
         break;
      case 4:
         printf("Goodbye\n");
         break;
      default:
         printf("Wrong Choice. Enter again\n");
         break;
      }

   } while (type != 4);
   // int c;

   // printf( "Enter a value :");
   // c = getchar( );

   // printf( "\nYou entered: ");
   // putchar( c );

   //testOpenAdd(TESTCASE2);

   //testMulHashing(TESTCASE2);

   return 0;
}

void testOpenAdd(int testCaseNum)
{

   // create file
   filehandle = createFile(FILESIZE, "openaddressing");

   int totalRecoreds = 0;

   if (testCaseNum == TESTCASE1)
   {
      // insert 20 record inside the file
      for (int i = 0; i < 20; i++)
      {
         totalRecoreds += insert(i + 1, i + 1, OPENADDRESSING);
      }
   }
   else
   {
      // insert 20 record inside the file
      for (int i = 0; i < 20; i++)
      {
         totalRecoreds += insert(i * 10, i + 1, OPENADDRESSING);
      }
   }

   printf("------------------------------------------------------------------------------\n");
   printf("Total Numbers of records searched to complete file is %d \n  ", totalRecoreds);
   printf("------------------------------------------------------------------------------\n");

   DisplayFile(filehandle);

   printf("------------------------------------------------------------------------------\n");

   search(10, OPENADDRESSING); // search for key 6

   printf("------------------------------------------------------------------------------\n");

   deleteItem(10, OPENADDRESSING); // then delete the record

   printf("------------------------------------------------------------------------------\n");

   insert(26, 26, OPENADDRESSING); // insert in the deleted record place

   printf("------------------------------------------------------------------------------\n");

   DisplayFile(filehandle);

   printf("------------------------------------------------------------------------------\n");

   insert(30, 30, OPENADDRESSING); // try to insert extra record in the full file

   close(filehandle);

   remove("openaddressing");
}

void testChaining(int testCaseNum)
{
   // create file
   printf("FILESIZECHAINING %d", FILESIZECHAINING);
   filehandle = createFile(FILESIZECHAINING, "chaining");

   int totalRecoreds = 0;

   if (testCaseNum == TESTCASE1)
   {
      // insert 20 record inside the file
      for (int i = 0; i < 20; i++)
      {
         totalRecoreds += insert(i + 1, i + 1, CHAINING);
      }
   }
   else
   {
      // insert 40 record inside the file
      for (int i = 0; i < 40; i++)
      {
         totalRecoreds += insert(i, i + 1, CHAINING);
      }
   }

   printf("------------------------------------------------------------------------------\n");
   printf("Total Numbers of records searched to complete file is %d \n  ", totalRecoreds);
   printf("------------------------------------------------------------------------------\n");

   DisplayFileChaining(filehandle);

   printf("------------------------------------------------------------------------------\n");

   search(10, CHAINING); // search for key 6

   printf("------------------------------------------------------------------------------\n");

   deleteItem(10, CHAINING); // then delete the record

   printf("------------------------------------------------------------------------------\n");

   insert(26, 26, CHAINING); // insert in the deleted record place

   printf("------------------------------------------------------------------------------\n");

   DisplayFileChaining(filehandle);

   printf("------------------------------------------------------------------------------\n");

   insert(30, 30, CHAINING);

   printf("------------------------------------------------------------------------------\n");

   DisplayFileChaining(filehandle);

   printf("------------------------------------------------------------------------------\n");

   close(filehandle);

   remove("chaining");
}

void testMulHashing(int testCaseNum)
{
   // create file
   filehandle = createFile(FILESIZE, "multihashing");

   int totalRecoreds = 0;

   if (testCaseNum == TESTCASE1)
   {
      // insert 20 record inside the file
      for (int i = 0; i < 20; i++)
      {
         totalRecoreds += insert(i + 1, i + 1, MULTIHASHING);
      }
   }
   else
   {
      // insert 20 record inside the file
      for (int i = 0; i < 20; i++)
      {
         totalRecoreds += insert(i * 10, i + 1, MULTIHASHING);
      }
   }

   printf("------------------------------------------------------------------------------\n");
   printf("Total Numbers of records searched to complete file is %d \n  ", totalRecoreds);
   printf("------------------------------------------------------------------------------\n");

   DisplayFile(filehandle);

   printf("------------------------------------------------------------------------------\n");

   search(10, MULTIHASHING); // search for key 6

   printf("------------------------------------------------------------------------------\n");

   deleteItem(10, MULTIHASHING); // then delete the record

   printf("------------------------------------------------------------------------------\n");

   insert(26, 26, MULTIHASHING); // insert in the deleted record place

   printf("------------------------------------------------------------------------------\n");

   DisplayFile(filehandle);

   printf("------------------------------------------------------------------------------\n");

   insert(30, 30, MULTIHASHING); // try to insert extra record in the full file

   close(filehandle);

   remove("multihashing");
}

/* functionality: insert the (key,data) pair into the database table
                  and print the number of comparisons it needed to find
    Input: key, data
    Output: print statement with the no. of comparisons
*/
int insert(int key, int data, int type)
{
   struct DataItem item;
   item.data = data;
   item.key = key;
   item.valid = 1;

   int result = 0;

   if (type == OPENADDRESSING)
   {
      result = insertItem(filehandle, item);
   }

   if (type == CHAINING)
   {
      result = insertItem_chaining(filehandle, item);
   }

   if (type == MULTIHASHING)
   {
      result = insertItem_multipleHashing(filehandle, item);
   }

   if (result == -2)
   {
      printf("Insert: No empty Space found in the file \n");
   }
   else if (result == -1)
   {
      printf("Insert: Error happened\n");
   }
   else
   {
      printf("Insert: No. of searched records:%d\n", abs(result));
   }
   return result;
}

/* Functionality: search for a data in the table using the key

   Input:  key
   Output: the return data Item
*/
struct DataItem *search(int key, int type)
{
   struct DataItem *item = (struct DataItem *)malloc(sizeof(struct DataItem));
   item->key = key;
   int diff = 0;

   int Offset;

   if (type == OPENADDRESSING)
   {
      Offset = searchItem(filehandle, item, &diff);
   }

   if (type == CHAINING)
   {
      Offset = searchItem_chaining(filehandle, item, &diff, false);
   }

   if (type == MULTIHASHING)
   {
      Offset = searchItem_multipleHashing(filehandle, item, &diff);
   }

   printf("Search: No of records searched is %d\n", diff);
   if (Offset < 0) //If offset is negative then the key doesn't exists in the table
      printf("Item not found\n");
   else
      printf("Item found at Offset: %d,  Data: %d and key: %d\n", Offset, item->data, item->key);
   return item;
}

/* Functionality: delete a record with a certain key

   Input:  key
   Output: return 1 on success and -1 on failure
*/
int deleteItem(int key, int type)
{
   struct DataItem *item = (struct DataItem *)malloc(sizeof(struct DataItem));
   item->key = key;
   int diff = 0;

   int Offset;
   if (type == OPENADDRESSING)
   {
      Offset = searchItem(filehandle, item, &diff);
   }

   if (type == CHAINING)
   {
      Offset = searchItem_chaining(filehandle, item, &diff, true);
   }

   if (type == MULTIHASHING)
   {
      Offset = searchItem_multipleHashing(filehandle, item, &diff);
   }
   printf("Delete: No of records searched is %d\n", diff);

   if(Offset >= 0 && type == CHAINING){
      return deleteChaining(filehandle, Offset, key);
   }

   if (Offset >= 0)
   {
      return deleteOffset(filehandle, Offset);
   }
   return -1;
}
