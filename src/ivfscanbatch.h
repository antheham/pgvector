#ifndef IVFSCANBATCH_H
#define IVFSCANBATCH_H

#include "postgres.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "utils/tuplesort.h"
#include "ivfflat.h"
#include "scanbatch.h"

#ifdef USE_CUDA
#include "cuda/cuda_wrapper.h"
#endif

/*
 * 批量扫描状态结构
 */
typedef struct IvfflatBatchScanOpaqueData
{
    IvfflatScanOpaqueData base;  /* 基础扫描状态 */
    ScanKeyBatch    batch_keys;   /* 批量查询键 */
    int             current_key;  /* 当前处理的键索引 */
    int             current_query_index;  /* 当前查询索引 */
    Tuplesortstate** sortstates;  /* 每个查询的排序状态数组 */
    TupleTableSlot** vslots;      /* 每个查询的虚拟槽位数组 */
    TupleTableSlot** mslots;      /* 每个查询的最小元组槽位数组 */
    
#ifdef USE_CUDA
    /* GPU批量处理支持 */
    bool            use_batch_gpu;        /* 是否使用批量GPU处理 */
    float*          gpu_batch_distances;   /* GPU批量距离结果 */
    int             batch_processed;       /* 已处理的批量数量 */
    
    /* GPU probes列表数据支持 */
    bool            probes_uploaded;       /* probes列表数据是否已上传到GPU */
    float*          gpu_probes_data;       /* GPU上的probes列表向量数据 */
    int*            gpu_probes_offsets;    /* GPU上的probes列表偏移量 */
    int*            gpu_probes_counts;     /* GPU上的probes列表计数 */
    int             total_probes_vectors;  /* 总probes向量数量 */
    ItemPointer*    probes_tids;           /* probes向量的TID数组 */
#endif
} IvfflatBatchScanOpaqueData;

typedef IvfflatBatchScanOpaqueData* IvfflatBatchScanOpaque;

/* 公共函数声明 */
extern IndexScanDesc ivfflatbatchbeginscan(Relation index, int nkeys, int norderbys, ScanKeyBatch batch_keys);
extern bool ivfflatbatchgettuple(IndexScanDesc scan, ScanDirection dir, Datum* values, bool* isnull, int max_tuples, int* returned_tuples, int k);
extern void ivfflatbatchendscan(IndexScanDesc scan);

/* 内部函数声明 */

#ifdef USE_CUDA
static void GetScanLists_BatchGPU(IndexScanDesc scan, ScanKeyBatch batch_keys);
static int UploadCentersToGPU_Batch(IndexScanDesc scan);
static int UploadProbesToGPU_Batch(IndexScanDesc scan);
static void GetScanItems_BatchGPU(IndexScanDesc scan, ScanKeyBatch batch_keys);
#endif

#endif /* IVFSCANBATCH_H */
