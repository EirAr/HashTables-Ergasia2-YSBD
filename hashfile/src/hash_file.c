#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

int open_files_array[MAX_OPEN_FILES];


HT_ErrorCode HT_Init() {
  for(int i = 0; i < MAX_OPEN_FILES; i++){
    open_files_array[i] = -1;
  }
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  int index, file_desc;
  char* first_block_data;
  BF_Block *first_block;
  BF_Block_Init(&first_block); //Init and allocating mem for the block
  CALL_BF(BF_CreateFile(filename)); //Creating a file
  CALL_BF(BF_OpenFile(filename, &file_desc)); //Opening existing file
  CALL_BF(BF_AllocateBlock(file_desc, first_block)); //Allocating a block, the first one in this case
  first_block_data = BF_Block_GetData(first_block);
  for(index = 0; index < BF_BLOCK_SIZE; index++){
    first_block_data[index] = '%';
  } //Is a HashFile only if its first block is filled with '%'
  BF_Block_SetDirty(first_block); //File was changed
  CALL_BF(BF_UnpinBlock(first_block));
  BF_Block_Destroy(&first_block); //Free mem


  int* hash_info_block_data;
  BF_Block *hash_info_block;
  BF_Block_Init(&hash_info_block); //Init and allocating mem for the block
  CALL_BF(BF_AllocateBlock(file_desc, hash_info_block)); //Allocating a block, the first one in this case
  hash_info_block_data = (int *)BF_Block_GetData(hash_info_block);

  int num_hash_blocks = (buckets % 128 == 0) ? (buckets / 128) : ((buckets / 128) + 1);
 
  hash_info_block_data[0] = buckets;
  hash_info_block_data[1] = num_hash_blocks;
  
  BF_Block_SetDirty(hash_info_block); //File was changed
  CALL_BF(BF_UnpinBlock(hash_info_block));
  BF_Block_Destroy(&hash_info_block); //Free mem


  BF_Block *hash_block;
  int* hash_block_data;

  for(int i = 0; i < num_hash_blocks; i++){
    BF_Block_Init(&hash_block);
    CALL_BF(BF_AllocateBlock(file_desc, hash_block));
    hash_block_data = (int*)(BF_Block_GetData(hash_block));
    for(i = 0; i < BF_BLOCK_SIZE/sizeof(int); i++){
      hash_block_data[i] = -1;
    }

    BF_Block_SetDirty(hash_block); //File was changed
    CALL_BF(BF_UnpinBlock(hash_block));
    BF_Block_Destroy(&hash_block);
  }



  CALL_BF(BF_CloseFile(file_desc));
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  char* block_data;
  BF_Block *block;
  BF_Block_Init(&block);
  int file_desc;

  CALL_BF(BF_OpenFile(fileName, &file_desc));
  CALL_BF(BF_GetBlock(file_desc, 0, block));
  block_data = BF_Block_GetData(block);
  for(int i = 0; i < BF_BLOCK_SIZE; i++){
    if(block_data[i] != '%'){ //Check if it is a HashFile
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      return HT_ERROR;
    }
  }
  for (int i = 0; i < MAX_OPEN_FILES; i++) {
    if(open_files_array[i] == -1) {
      open_files_array[i] = file_desc;
      *indexDesc = i;
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      return HT_OK;
    }
  }
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return HT_ERROR;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  if(indexDesc >= MAX_OPEN_FILES || indexDesc < 0 || open_files_array[indexDesc] == -1){
    return HT_ERROR;
  }
  CALL_BF(BF_CloseFile(open_files_array[indexDesc]));
  open_files_array[indexDesc] = -1;
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  if(indexDesc >= MAX_OPEN_FILES || indexDesc < 0 || open_files_array[indexDesc] == -1){
    return HT_ERROR;
  }
  int file_desc = open_files_array[indexDesc];//file descriptor

  BF_Block *hash_info_block;
  BF_Block_Init(&hash_info_block);
  CALL_BF(BF_GetBlock(file_desc, 1, hash_info_block));
  int* hash_info_block_data = (int*)(BF_Block_GetData(hash_info_block));
  int buckets = hash_info_block_data[0];

  int hash_value = record.id % buckets;
  int hash_block_num = (hash_value / 128) + 2;
  int hash_bucket_num = hash_value % 128;

  BF_Block *hash_block;
  BF_Block_Init(&hash_block);
  CALL_BF(BF_GetBlock(file_desc, hash_block_num, hash_block));
  int* hash_block_data = (int*)(BF_Block_GetData(hash_block));

  int records_num;

  BF_Block *block_of_records;
  BF_Block_Init(&block_of_records);
  if ( hash_block_data[hash_bucket_num] == -1 ) {

    CALL_BF(BF_AllocateBlock(file_desc, block_of_records));
    int* block_of_records_data = (int*)(BF_Block_GetData(block_of_records));
    records_num = 1;
    memcpy(block_of_records_data, &records_num, sizeof(int));
    block_of_records_data = block_of_records_data + 1;
    int next_block = -1;
    memcpy(block_of_records_data, &next_block, sizeof(int)); /*setting the last 4bytes to -1, to indicate that there is no next block*/
    block_of_records_data = block_of_records_data + 1;
    memcpy(block_of_records_data, &record, sizeof(Record));
    
    int blocks_num;
    CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));
    hash_block_data[hash_bucket_num] = blocks_num - 1;

    BF_Block_SetDirty(block_of_records);
    CALL_BF(BF_UnpinBlock(block_of_records));
    BF_Block_Destroy(&block_of_records);

    BF_Block_SetDirty(hash_block);
  } else {


    CALL_BF(BF_GetBlock(file_desc, hash_block_data[hash_bucket_num], block_of_records));
    int* block_of_records_data = (int*)(BF_Block_GetData(block_of_records));

    int block_num = block_of_records_data[1];

    while (block_num != -1) {
      CALL_BF(BF_UnpinBlock(block_of_records));

      CALL_BF(BF_GetBlock(file_desc, block_num, block_of_records));
      block_of_records_data = (int*)(BF_Block_GetData(block_of_records));

      block_num = block_of_records_data[1];
    }

    int max_records_in_block = (BF_BLOCK_SIZE - (2 * sizeof(int)))/sizeof(Record);
    int records_num = block_of_records_data[0];
    if (records_num == max_records_in_block) {

      int blocks_num;
      CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));
      block_of_records_data[1] = blocks_num; // Change the next block from -1 to the next available block in file

      CALL_BF(BF_UnpinBlock(block_of_records));
      CALL_BF(BF_AllocateBlock(file_desc, block_of_records));
      block_of_records_data = (int*)(BF_Block_GetData(block_of_records));
      block_of_records_data[0] = 1;
      block_of_records_data[1] = -1;
      block_of_records_data = block_of_records_data + 2; // Point to the first record in block
      memcpy(block_of_records_data, &record, sizeof(Record));

      

      BF_Block_SetDirty(block_of_records);
      CALL_BF(BF_UnpinBlock(block_of_records));
      BF_Block_Destroy(&block_of_records);
    } else {
      (block_of_records_data[0])++;
      block_of_records_data = block_of_records_data + 2; // Point to the first record in block
      block_of_records_data = (int *)((Record*)(block_of_records_data) + records_num);  // Point after the last record of the block
      memcpy(block_of_records_data, &record, sizeof(Record));

      BF_Block_SetDirty(block_of_records);
      CALL_BF(BF_UnpinBlock(block_of_records));
      BF_Block_Destroy(&block_of_records);
    }

  }

  CALL_BF(BF_UnpinBlock(hash_block));
  BF_Block_Destroy(&hash_block);

  CALL_BF(BF_UnpinBlock(hash_info_block));
  BF_Block_Destroy(&hash_info_block);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  
  // Get the file descriptor
  int file_desc = open_files_array[indexDesc]; 
  
  // Get the number of hashing blocks, to know where to start the printing
  BF_Block *hash_info_block;
  BF_Block_Init(&hash_info_block);
  CALL_BF(BF_GetBlock(file_desc, 1, hash_info_block));
  int* hash_info_block_data = (int*)(BF_Block_GetData(hash_info_block));
  int num_hash_blocks = hash_info_block_data[1];
  CALL_BF(BF_UnpinBlock(hash_info_block));
  BF_Block_Destroy(&hash_info_block);

  // Get the number of blocks to know where to stop the printing
  int blocks_num;
  CALL_BF(BF_GetBlockCounter(file_desc, &blocks_num));


  BF_Block *block_of_records;
  BF_Block_Init(&block_of_records);
  Record *record;
  int records_num;
  if(id == NULL){
    for(int i = num_hash_blocks + 2; i < blocks_num; i++){
      CALL_BF(BF_GetBlock(file_desc, i, block_of_records));
      int* block_of_records_data = (int*)(BF_Block_GetData(block_of_records));

      memcpy(&records_num, block_of_records_data, sizeof(int));
      record = (Record*)(block_of_records_data + 2 * sizeof(int));
      for(int j = 0; j < records_num; j++){
        printf("Id: %d Name: %s Surname: %s City: %s\n", record[j].id, record[j].name, record[j].surname, record[j].city);
      }
      CALL_BF(BF_UnpinBlock(block_of_records));
    }
  }



  BF_Block_Destroy(&block_of_records);
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  //insert code here
  return HT_OK;
}






