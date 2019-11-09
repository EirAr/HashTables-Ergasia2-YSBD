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

HT_ErrorCode HT_Init() {
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
  } //Is a HeapFile only if its first block is filled with '%'
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
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  //insert code here
  return HT_OK;
}
