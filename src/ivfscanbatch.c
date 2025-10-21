/* ivfscanbatch.c */
#include "postgres.h"
#include "access/relscan.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/relscan.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "lib/pairingheap.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/float.h"
#include "utils/memutils.h"
#include <float.h>
#include "utils/sortsupport.h"
#include "catalog/pg_operator.h"
#include "utils/guc.h"
#include "ivfflat.h"
#include "scanbatch.h"
#include "ivfscanbatch.h"



/*
 * 初始化扫描排序状态
 */
static Tuplesortstate *
InitScanSortState(TupleDesc tupdesc)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, 0, NULL, false);
}

/*
 * 比较列表距离
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
    const IvfflatScanList *listA = (const IvfflatScanList *) a;
    const IvfflatScanList *listB = (const IvfflatScanList *) b;
    
    if (listA->distance < listB->distance)
        return -1;
    else if (listA->distance > listB->distance)
        return 1;
    else
        return 0;
}

/*
 * 检查批量查询中是否包含NULL值
 */
static bool
HasNullQueries(ScanKeyBatch batch_keys)
{
    for (int i = 0; i < batch_keys->nkeys; i++) {
        Datum value = ScanKeyBatchGetVector(batch_keys, i);
        if (DatumGetPointer(value) == NULL) {
            return true;
        }
    }
    return false;
}

/*
 * 从配对堆节点获取扫描列表
 */
static IvfflatScanList *
GetScanList(const pairingheap_node *node)
{
    return (IvfflatScanList *) node;
}

/*
 * 初始化批量扫描
 */
IndexScanDesc
ivfflatbatchbeginscan(Relation index, int nkeys, int norderbys, ScanKeyBatch batch_keys)
{
    elog(LOG, "ivfflatbatchbeginscan: 函数开始执行");
    
    IndexScanDesc scan;
    IvfflatBatchScanOpaque so;
    MemoryContext oldCtx;
    int         lists, dimensions;

    elog(LOG, "ivfflatbatchbeginscan: 批量查询数量=%d, nkeys=%d, norderbys=%d", 
         batch_keys->nkeys, nkeys, norderbys);

    /* 检查批量查询中是否包含NULL值 */
    if (HasNullQueries(batch_keys)) {
        elog(ERROR, "ivfflatbatchbeginscan: 批量向量搜索不支持NULL值查询，请使用单个查询或过滤NULL值");
    }

    /* 创建扫描描述符 */
    scan = RelationGetIndexScan(index, nkeys, norderbys);
    elog(LOG, "ivfflatbatchbeginscan: 扫描描述符创建成功, scan=%p", scan);

    /* 获取列表数和维度数 */
    IvfflatGetMetaPageInfo(index, &lists, &dimensions);
    elog(LOG, "ivfflatbatchbeginscan: 获取元数据成功, lists=%d, dimensions=%d", lists, dimensions);

    /* 分配扫描状态 - 在CurrentMemoryContext中分配，确保scan->opaque有效 */
    /* 注意：这里使用CurrentMemoryContext，因为ivfflatbatchbeginscan是从batch_vector_search_c调用的 */
    /* 而batch_vector_search_c在funcctx->multi_call_memory_ctx中运行 */
    so = (IvfflatBatchScanOpaque)palloc0(sizeof(IvfflatBatchScanOpaqueData));
    elog(LOG, "ivfflatbatchbeginscan: 批量扫描状态分配成功, so=%p, sizeof(IvfflatBatchScanOpaqueData)=%zu, sizeof(IvfflatScanOpaqueData)=%zu", 
         so, sizeof(IvfflatBatchScanOpaqueData), sizeof(IvfflatScanOpaqueData));

    /* 初始化基础扫描状态 */
    so->base.typeInfo = IvfflatGetTypeInfo(index);
    so->base.first = true;
    so->base.probes = ivfflat_probes;
    so->base.maxProbes = (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF) ?
        Max(ivfflat_max_probes, ivfflat_probes) : ivfflat_probes;
    so->base.dimensions = dimensions;

    elog(LOG, "ivfflatbatchbeginscan: 基础扫描状态初始化完成, probes=%d, maxProbes=%d", 
         so->base.probes, so->base.maxProbes);

    /* 设置支持函数 */
    so->base.procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
    so->base.normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
    so->base.collation = index->rd_indcollation[0];
    elog(LOG, "ivfflatbatchbeginscan: 支持函数设置完成");

    /* 创建内存上下文 - 使用CurrentMemoryContext，确保与调用者一致 */
    so->base.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
        "Ivfflat batch scan temporary context",
        ALLOCSET_DEFAULT_SIZES);
    elog(LOG, "ivfflatbatchbeginscan: 内存上下文创建成功, tmpCtx=%p", so->base.tmpCtx);

    /* 设置scan->opaque - 在内存上下文切换前设置，确保指针有效 */
    scan->opaque = so;
    elog(LOG, "ivfflatbatchbeginscan: 设置scan->opaque完成, scan=%p, opaque=%p", scan, scan->opaque);

    /* 存储批量键 */
    so->batch_keys = batch_keys;
    so->current_key = 0;
    so->current_query_index = 0;
    elog(LOG, "ivfflatbatchbeginscan: 批量键存储完成 - so->batch_keys: %p, batch_keys: %p, nkeys: %d", 
         so->batch_keys, batch_keys, batch_keys->nkeys);

    /* 为每个查询创建排序状态和槽位 - 在CurrentMemoryContext中分配，确保指针有效 */
    int nbatch = batch_keys->nkeys;
    so->sortstates = palloc(nbatch * sizeof(Tuplesortstate*));
    so->vslots = palloc(nbatch * sizeof(TupleTableSlot*));
    so->mslots = palloc(nbatch * sizeof(TupleTableSlot*));

    /* 创建元组描述符 */
    so->base.tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(so->base.tupdesc, (AttrNumber)1, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->base.tupdesc, (AttrNumber)2, "heaptid", TIDOID, -1, 0);

    /* 初始化每个查询的状态 */
    for (int i = 0; i < nbatch; i++)
    {
        so->sortstates[i] = InitScanSortState(so->base.tupdesc);
        so->vslots[i] = MakeSingleTupleTableSlot(so->base.tupdesc, &TTSOpsVirtual);
        so->mslots[i] = MakeSingleTupleTableSlot(so->base.tupdesc, &TTSOpsMinimalTuple);
        elog(LOG, "ivfflatbatchbeginscan: 初始化查询 %d 状态完成", i);
    }

    /* 设置缓冲区访问策略 */
    // so->base.bas = GetAccessStrategy(BAS_BULKREAD);

    /* 初始化列表队列 */
    so->base.listQueue = pairingheap_allocate(CompareLists, scan);
    so->base.listPages = palloc(so->base.maxProbes * sizeof(BlockNumber));
    so->base.listIndex = 0;
    so->base.lists = palloc(so->base.maxProbes * sizeof(IvfflatScanList));

    /* 切换到tmpCtx进行后续操作 */
    oldCtx = MemoryContextSwitchTo(so->base.tmpCtx);

#ifdef USE_CUDA
    /* 初始化GPU批量处理支持 */
    so->use_batch_gpu = false;
    so->gpu_batch_distances = NULL;
    so->batch_processed = 0;
    
    /* 初始化GPU probes相关字段 */
    so->probes_uploaded = false;
    so->gpu_probes_data = NULL;
    so->gpu_probes_offsets = NULL;
    so->gpu_probes_counts = NULL;
    so->total_probes_vectors = 0;
    
    /* 检查CUDA是否可用，如果没有GPU则直接报错 */
    if (!cuda_is_available()) {
        elog(ERROR, "批量向量搜索需要GPU支持，但CUDA不可用");
    }
    
    /* 初始化基础GPU支持 */
    so->base.use_gpu = false;
    so->base.centers_uploaded = false;
    so->base.cuda_ctx = NULL;
    so->base.gpu_distances = NULL;
    
    /* 初始化CUDA上下文 */
    elog(LOG, "ivfflatbatchbeginscan: 开始初始化CUDA上下文, lists=%d, dimensions=%d", lists, dimensions);
    so->base.cuda_ctx = cuda_center_search_init(lists, dimensions, false);
    if (!so->base.cuda_ctx) {
        elog(ERROR, "无法初始化CUDA上下文，批量向量搜索需要GPU支持");
    }
    elog(LOG, "ivfflatbatchbeginscan: CUDA上下文初始化成功, ctx=%p", so->base.cuda_ctx);
    
    so->base.use_gpu = true;
    so->use_batch_gpu = true;
    so->base.gpu_distances = palloc(lists * sizeof(float));
    so->gpu_batch_distances = palloc(lists * nbatch * sizeof(float));
    elog(INFO, "批量GPU聚类中心搜索已启用");
    
    /* 延迟上传聚类中心数据到GPU，在第一次使用时上传 */
    elog(LOG, "ivfflatbatchbeginscan: GPU初始化完成，将在第一次使用时上传聚类中心数据");
#endif

    MemoryContextSwitchTo(oldCtx);

    elog(LOG, "ivfflatbatchbeginscan: 函数即将返回, tmpCtx=%p", so->base.tmpCtx);
    return scan;
}

/*
 * 批量扫描获取多个元组
 */
bool
ivfflatbatchgettuple(IndexScanDesc scan, ScanDirection dir, Datum* values, bool* isnull, int max_tuples, int* returned_tuples, int k)
{
    elog(LOG, "ivfflatbatchgettuple: 函数开始执行");
    
    /* 检查scan和opaque指针 */
    if (!scan) {
        elog(ERROR, "ivfflatbatchgettuple: scan指针为空");
    }
    if (!scan->opaque) {
        elog(ERROR, "ivfflatbatchgettuple: scan->opaque指针为空");
    }
    
    elog(LOG, "ivfflatbatchgettuple: scan指针: %p, scan->opaque指针: %p", scan, scan->opaque);
    
    /* 检查opaque指针的有效性 */
    if ((uintptr_t)scan->opaque < 0x1000 || (uintptr_t)scan->opaque > 0x7fffffffffffffff) {
        elog(ERROR, "ivfflatbatchgettuple: scan->opaque指针无效: %p", scan->opaque);
    }
    
    elog(LOG, "ivfflatbatchgettuple: 开始类型转换 scan->opaque");
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "ivfflatbatchgettuple: 类型转换完成，so: %p", so);
    
    /* 检查so结构体的关键字段 */
    elog(LOG, "ivfflatbatchgettuple: 检查so结构体字段");
    if (!so) {
        elog(ERROR, "ivfflatbatchgettuple: so指针为空");
    }
    
    elog(LOG, "ivfflatbatchgettuple: 检查so->batch_keys，so地址: %p", so);
    if (!so->batch_keys) {
        elog(ERROR, "ivfflatbatchgettuple: so->batch_keys指针为空，so地址: %p", so);
    }
    elog(LOG, "ivfflatbatchgettuple: so->batch_keys地址: %p", so->batch_keys);
    elog(LOG, "ivfflatbatchgettuple: so->batch_keys检查通过，地址: %p", so->batch_keys);
    
    elog(LOG, "ivfflatbatchgettuple: 检查so->base字段");
    
    /* 检查tmpCtx指针有效性 */
    if (!so->base.tmpCtx) {
        elog(ERROR, "ivfflatbatchgettuple: so->base.tmpCtx指针为空");
    }
    elog(LOG, "ivfflatbatchgettuple: so->base.tmpCtx检查通过，地址: %p", so->base.tmpCtx);
    
    elog(LOG, "ivfflatbatchgettuple: 检查so->sortstates");
    if (!so->sortstates) {
        elog(ERROR, "ivfflatbatchgettuple: so->sortstates指针为空");
    }
    elog(LOG, "ivfflatbatchgettuple: so->sortstates检查通过，地址: %p", so->sortstates);
    
    elog(LOG, "ivfflatbatchgettuple: 检查so->vslots");
    if (!so->vslots) {
        elog(ERROR, "ivfflatbatchgettuple: so->vslots指针为空");
    }
    elog(LOG, "ivfflatbatchgettuple: so->vslots检查通过，地址: %p", so->vslots);
    
    elog(LOG, "ivfflatbatchgettuple: 检查so->mslots");
    if (!so->mslots) {
        elog(ERROR, "ivfflatbatchgettuple: so->mslots指针为空");
    }
    elog(LOG, "ivfflatbatchgettuple: so->mslots检查通过，地址: %p", so->mslots);
    
    elog(LOG, "ivfflatbatchgettuple: 所有检查通过，继续执行");
    
    if (!so->batch_keys) {
        elog(ERROR, "ivfflatbatchgettuple: so->batch_keys指针为空");
    }
    
    elog(LOG, "ivfflatbatchgettuple: 开始访问so->batch_keys->nkeys");
    elog(LOG, "ivfflatbatchgettuple: so->batch_keys指针: %p", so->batch_keys);
    
    if (!so->batch_keys) {
        elog(ERROR, "ivfflatbatchgettuple: so->batch_keys指针为空");
    }
    
    elog(LOG, "ivfflatbatchgettuple: so->batch_keys->nkeys值: %d", so->batch_keys->nkeys);
    int nbatch = so->batch_keys->nkeys;
    elog(LOG, "ivfflatbatchgettuple: 成功获取批量查询数量: %d", nbatch);
    
    int count = 0;
    
    elog(LOG, "ivfflatbatchgettuple: 参数 - nbatch: %d, max_tuples: %d", nbatch, max_tuples);
    elog(LOG, "ivfflatbatchgettuple: 扫描状态检查 - current_key: %d", so->current_key);
#ifdef USE_CUDA
    elog(LOG, "ivfflatbatchgettuple: GPU状态 - probes_uploaded: %s, probes_tids: %p", 
         so->probes_uploaded ? "是" : "否", so->probes_tids);
#endif

    /* 确保向前扫描 */
    Assert(ScanDirectionIsForward(dir));

    /* 初始化返回数组 */
    for (int i = 0; i < max_tuples; i++)
    {
        values[i] = (Datum)0;
        isnull[i] = true;
    }

    /* 如果还没有处理所有查询，先进行处理 */
    elog(LOG, "ivfflatbatchgettuple: 检查处理条件 - current_key: %d, nbatch: %d", so->current_key, nbatch);
    if (so->current_key == 0)
    {
        elog(LOG, "ivfflatbatchgettuple: 第一次调用，进入GPU处理分支");
        elog(LOG, "ivfflatbatchgettuple: 准备调用GPU函数，scan: %p, batch_keys: %p", scan, so->batch_keys);
#ifdef USE_CUDA
        elog(LOG, "ivfflatbatchgettuple: 检查GPU状态 - use_batch_gpu: %s", so->use_batch_gpu ? "是" : "否");
        /* 使用GPU批量处理所有查询 */
        if (!so->use_batch_gpu) {
            elog(ERROR, "批量向量搜索需要GPU支持，但GPU不可用");
        }
        
        elog(LOG, "ivfflatbatchgettuple: 检查聚类中心上传状态 - centers_uploaded: %s", so->base.centers_uploaded ? "是" : "否");
        /* 如果聚类中心未上传，先上传 */
        if (!so->base.centers_uploaded) {
            elog(LOG, "ivfflatbatchgettuple: 聚类中心未上传，现在开始上传");
            elog(LOG, "ivfflatbatchgettuple: 调用UploadCentersToGPU_Batch前");
            if (UploadCentersToGPU_Batch(scan) != 0) {
                elog(ERROR, "ivfflatbatchgettuple: 无法上传聚类中心数据到GPU，批量向量搜索需要GPU支持");
            }
            elog(LOG, "ivfflatbatchgettuple: UploadCentersToGPU_Batch完成");
        }
        
        elog(LOG, "ivfflatbatchgettuple: 开始调用GetScanLists_BatchGPU");
        elog(LOG, "ivfflatbatchgettuple: GetScanLists_BatchGPU调用前 - scan: %p, batch_keys: %p", scan, so->batch_keys);
        GetScanLists_BatchGPU(scan, so->batch_keys);
        elog(LOG, "ivfflatbatchgettuple: GetScanLists_BatchGPU完成");
        
        elog(LOG, "ivfflatbatchgettuple: 开始调用GetScanItems_BatchGPU");
        elog(LOG, "ivfflatbatchgettuple: GetScanItems_BatchGPU调用前 - scan: %p, batch_keys: %p", scan, so->batch_keys);
        GetScanItems_BatchGPU(scan, so->batch_keys);
        elog(LOG, "ivfflatbatchgettuple: GetScanItems_BatchGPU完成");
        so->current_key = 1; /* 标记GPU处理已完成，但保持为1以便后续调用 */
        elog(LOG, "ivfflatbatchgettuple: GPU处理完成，current_key设置为1");
#else
        elog(ERROR, "批量向量搜索需要GPU支持，但未编译CUDA支持");
#endif
    }

    /* 处理所有查询的结果 */
    elog(LOG, "ivfflatbatchgettuple: 开始处理所有查询，nbatch: %d, max_tuples: %d", nbatch, max_tuples);
    
    /* 为每个查询向量获取最多k个结果 */
    int max_results_per_query = k; // 直接使用用户指定的k值
    int total_max_results = max_results_per_query * nbatch; // 总的最大结果数
    
    for (int query_idx = 0; query_idx < nbatch && count < total_max_results; query_idx++) {
        elog(LOG, "ivfflatbatchgettuple: 处理查询 %d/%d", query_idx + 1, nbatch);
        
        /* 获取当前查询的排序状态 */
        Tuplesortstate* sortstate = so->sortstates[query_idx];
        TupleTableSlot* mslot = so->mslots[query_idx];
        ItemPointer heaptid;
        float8 distance;
        bool        isnull_local;

        /* 从排序状态获取元组 */
        elog(LOG, "ivfflatbatchgettuple: 准备调用tuplesort_gettupleslot，查询 %d", query_idx);
        elog(LOG, "ivfflatbatchgettuple: 检查指针有效性 - sortstate: %p, mslot: %p", sortstate, mslot);
        
        if (!sortstate) {
            elog(ERROR, "ivfflatbatchgettuple: sortstate指针为空，查询 %d", query_idx);
        }
        if (!mslot) {
            elog(ERROR, "ivfflatbatchgettuple: mslot指针为空，查询 %d", query_idx);
        }
        
        elog(LOG, "ivfflatbatchgettuple: 指针检查通过，开始调用tuplesort_gettupleslot，查询 %d", query_idx);
        
        /* 为当前查询向量获取最多k个结果 */
        int query_result_count = 0;
        
        while (query_result_count < max_results_per_query && count < total_max_results && tuplesort_gettupleslot(sortstate, true, false, mslot, NULL))
    {
        elog(LOG, "ivfflatbatchgettuple: 从排序状态获取到元组，当前查询 - scan: %p, opaque: %p", scan, scan->opaque);
        
        /* 获取距离和tid */
        Datum distance_datum = slot_getattr(mslot, 1, &isnull_local);
        elog(LOG, "ivfflatbatchgettuple: 距离属性获取完成，isnull: %s", isnull_local ? "是" : "否");
        
        if (!isnull_local) {
            distance = DatumGetFloat8(distance_datum);
            elog(LOG, "ivfflatbatchgettuple: 距离值: %f", distance);
        } else {
            elog(LOG, "ivfflatbatchgettuple: 距离值为空，跳过");
            continue;
        }
        
        Datum tid_datum = slot_getattr(mslot, 2, &isnull_local);
        elog(LOG, "ivfflatbatchgettuple: TID属性获取完成，isnull: %s", isnull_local ? "是" : "否");
        
        if (!isnull_local) {
            heaptid = (ItemPointer)DatumGetPointer(tid_datum);
            elog(LOG, "ivfflatbatchgettuple: TID指针: %p", heaptid);
            
            if (heaptid) {
                elog(LOG, "ivfflatbatchgettuple: TID值: 页号=%d, 偏移=%d", 
                     BlockIdGetBlockNumber(&heaptid->ip_blkid), ItemPointerGetOffsetNumber(heaptid));
            } else {
                elog(LOG, "ivfflatbatchgettuple: TID指针为空，跳过");
                continue;
            }
        } else {
            elog(LOG, "ivfflatbatchgettuple: TID值为空，跳过");
            continue;
        }

        /* 创建元组槽位数据 - 模仿ivfscan.c的模式 */
        elog(LOG, "ivfflatbatchgettuple: 准备创建元组槽位数据，当前查询");
        
        // 创建元组槽位数据 - 模仿GetScanItems_GPU的模式
        // 第0列：距离值
        // 第1列：TID指针
        Datum tuple_values[2];
        bool tuple_nulls[2] = {false, false};
        
        tuple_values[0] = Float8GetDatum(distance);  // 距离值
        tuple_values[1] = PointerGetDatum(heaptid);  // TID指针
        
        elog(LOG, "ivfflatbatchgettuple: 元组槽位数据创建成功，当前查询");

        /* 存储结果 */
        elog(LOG, "ivfflatbatchgettuple: 准备存储结果 %d - scan: %p, opaque: %p", count, scan, scan->opaque);
        values[count] = PointerGetDatum(tuple_values);
        isnull[count] = false;
        count++;
        elog(LOG, "ivfflatbatchgettuple: 结果 %d 存储完成 - scan: %p, opaque: %p", count, scan, scan->opaque);
        
        elog(LOG, "ivfflatbatchgettuple: 结果 %d 创建完成", count);
        query_result_count++;
    }
    
    elog(LOG, "ivfflatbatchgettuple: 查询 %d 处理完成，获得 %d 个结果", query_idx + 1, query_result_count);
    } // 结束查询循环

    elog(LOG, "ivfflatbatchgettuple: 所有循环完成，总结果数: %d - scan: %p, opaque: %p, tmpCtx: %p", count, scan, scan->opaque, so->base.tmpCtx);
    
    // 详细调试：分析返回结果的列数和数据
    elog(LOG, "ivfflatbatchgettuple: 调试信息 - nbatch: %d, max_tuples: %d, count: %d", nbatch, max_tuples, count);
    elog(LOG, "ivfflatbatchgettuple: 每个查询向量应该返回的结果数: %d", max_results_per_query);
    
    // 检查每个结果的数据结构
    for (int debug_i = 0; debug_i < count && debug_i < 5; debug_i++) {
        if (!isnull[debug_i]) {
            // values[debug_i] 是一个指向 Datum 数组的指针
            Datum *debug_tuple_values = (Datum*)DatumGetPointer(values[debug_i]);
            
            // 第0列：距离值
            float8 debug_distance = DatumGetFloat8(debug_tuple_values[0]);
            
            // 第1列：TID指针
            ItemPointer debug_heaptid = (ItemPointer)DatumGetPointer(debug_tuple_values[1]);
            
            elog(LOG, "ivfflatbatchgettuple: 结果 %d - 页号: %u, 偏移: %u, 距离: %.15f", 
                 debug_i, 
                 BlockIdGetBlockNumber(&debug_heaptid->ip_blkid),
                 ItemPointerGetOffsetNumber(debug_heaptid),
                 debug_distance);
        } else {
            elog(LOG, "ivfflatbatchgettuple: 结果 %d - 为NULL", debug_i);
        }
    }
    
    *returned_tuples = count;
    
    /* 在返回前再次检查tmpCtx */
    elog(LOG, "ivfflatbatchgettuple: 返回前最后检查 - tmpCtx: %p, listQueue: %p, listPages: %p", 
         so->base.tmpCtx, so->base.listQueue, so->base.listPages);
    
    elog(LOG, "ivfflatbatchgettuple: 函数即将返回，count > 0: %s", (count > 0) ? "是" : "否");
    return (count > 0);
}

/*
 * 结束批量扫描
 */
void
ivfflatbatchendscan(IndexScanDesc scan)
{
    elog(LOG, "ivfflatbatchendscan: 开始清理扫描");
    
    if (!scan) {
        elog(ERROR, "ivfflatbatchendscan: scan 为空");
    }
    
    if (!scan->opaque) {
        elog(ERROR, "ivfflatbatchendscan: scan->opaque 为空");
    }
    
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "ivfflatbatchendscan: 获取 opaque 成功");
    
    if (!so->batch_keys) {
        elog(ERROR, "ivfflatbatchendscan: so->batch_keys 为空");
    }
    
    int nbatch = so->batch_keys->nkeys;
    elog(LOG, "ivfflatbatchendscan: nbatch = %d", nbatch);

    /* 释放每个查询的排序状态和槽位 */
    for (int i = 0; i < nbatch; i++)
    {
        elog(LOG, "ivfflatbatchendscan: 释放查询 %d - sortstate: %p, vslot: %p, mslot: %p", 
             i, so->sortstates[i], so->vslots[i], so->mslots[i]);
        
        if (so->sortstates[i]) {
            elog(LOG, "ivfflatbatchendscan: 调用tuplesort_end，查询 %d", i);
            tuplesort_end(so->sortstates[i]);
            elog(LOG, "ivfflatbatchendscan: tuplesort_end完成，查询 %d", i);
        }
        if (so->vslots[i]) {
            elog(LOG, "ivfflatbatchendscan: 调用ExecDropSingleTupleTableSlot(vslot)，查询 %d", i);
            ExecDropSingleTupleTableSlot(so->vslots[i]);
            elog(LOG, "ivfflatbatchendscan: ExecDropSingleTupleTableSlot(vslot)完成，查询 %d", i);
        }
        if (so->mslots[i]) {
            elog(LOG, "ivfflatbatchendscan: 调用ExecDropSingleTupleTableSlot(mslot)，查询 %d", i);
            ExecDropSingleTupleTableSlot(so->mslots[i]);
            elog(LOG, "ivfflatbatchendscan: ExecDropSingleTupleTableSlot(mslot)完成，查询 %d", i);
        }
        elog(LOG, "ivfflatbatchendscan: 查询 %d 释放完成", i);
    }

    /* 释放数组 */
    elog(LOG, "ivfflatbatchendscan: 开始释放数组 - sortstates: %p, vslots: %p, mslots: %p", 
         so->sortstates, so->vslots, so->mslots);
    
    elog(LOG, "ivfflatbatchendscan: 调用pfree(so->sortstates)");
    pfree(so->sortstates);
    elog(LOG, "ivfflatbatchendscan: pfree(so->sortstates)完成");
    
    elog(LOG, "ivfflatbatchendscan: 调用pfree(so->vslots)");
    pfree(so->vslots);
    elog(LOG, "ivfflatbatchendscan: pfree(so->vslots)完成");
    
    elog(LOG, "ivfflatbatchendscan: 调用pfree(so->mslots)");
    pfree(so->mslots);
    elog(LOG, "ivfflatbatchendscan: pfree(so->mslots)完成");

    /* 释放基础资源 */
    elog(LOG, "ivfflatbatchendscan: 开始释放基础资源 - listQueue: %p, listPages: %p, lists: %p", 
         so->base.listQueue, so->base.listPages, so->base.lists);
    
    /* 检查指针有效性 - 更严格的检查 */
    if ((uintptr_t)so->base.listQueue < 0x1000000 || (uintptr_t)so->base.listQueue > 0x7fffffffffff) {
        elog(WARNING, "ivfflatbatchendscan: listQueue指针无效: %p，跳过释放", so->base.listQueue);
    } else {
        elog(LOG, "ivfflatbatchendscan: 调用pairingheap_free(so->base.listQueue)");
        pairingheap_free(so->base.listQueue);
        elog(LOG, "ivfflatbatchendscan: pairingheap_free(so->base.listQueue)完成");
    }
    
    if ((uintptr_t)so->base.listPages < 0x1000000 || (uintptr_t)so->base.listPages > 0x7fffffffffff) {
        elog(WARNING, "ivfflatbatchendscan: listPages指针无效: %p，跳过释放", so->base.listPages);
    } else {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->base.listPages)");
        pfree(so->base.listPages);
        elog(LOG, "ivfflatbatchendscan: pfree(so->base.listPages)完成");
    }
    
    if ((uintptr_t)so->base.lists < 0x1000000 || (uintptr_t)so->base.lists > 0x7fffffffffff) {
        elog(WARNING, "ivfflatbatchendscan: lists指针无效: %p，跳过释放", so->base.lists);
    } else {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->base.lists) - 地址: %p", so->base.lists);
        pfree(so->base.lists);
        so->base.lists = NULL;  // 防止重复释放
        elog(LOG, "ivfflatbatchendscan: pfree(so->base.lists)完成");
    }

#ifdef USE_CUDA
    /* 清理GPU资源 */
    elog(LOG, "ivfflatbatchendscan: 开始清理GPU资源");
    
    if (so->base.cuda_ctx) {
        elog(LOG, "ivfflatbatchendscan: 调用cuda_center_search_cleanup");
        cuda_center_search_cleanup((CudaCenterSearchContext*)so->base.cuda_ctx);
        so->base.cuda_ctx = NULL;
        elog(LOG, "ivfflatbatchendscan: cuda_center_search_cleanup完成");
    }
    
    if (so->base.gpu_distances) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->base.gpu_distances) - 地址: %p", so->base.gpu_distances);
        pfree(so->base.gpu_distances);
        so->base.gpu_distances = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->base.gpu_distances)完成");
    }
    
    if (so->gpu_batch_distances) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->gpu_batch_distances)");
        pfree(so->gpu_batch_distances);
        so->gpu_batch_distances = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->gpu_batch_distances)完成");
    }
    
    /* 清理GPU probes相关资源 */
    if (so->gpu_probes_data) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->gpu_probes_data)");
        pfree(so->gpu_probes_data);
        so->gpu_probes_data = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->gpu_probes_data)完成");
    }
    
    if (so->gpu_probes_offsets) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->gpu_probes_offsets)");
        pfree(so->gpu_probes_offsets);
        so->gpu_probes_offsets = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->gpu_probes_offsets)完成");
    }
    
    if (so->gpu_probes_counts) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->gpu_probes_counts)");
        pfree(so->gpu_probes_counts);
        so->gpu_probes_counts = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->gpu_probes_counts)完成");
    }
    
    if (so->probes_tids) {
        elog(LOG, "ivfflatbatchendscan: 调用pfree(so->probes_tids)");
        pfree(so->probes_tids);
        so->probes_tids = NULL;
        elog(LOG, "ivfflatbatchendscan: pfree(so->probes_tids)完成");
    }
    
    elog(LOG, "ivfflatbatchendscan: GPU资源清理完成");
#endif

    elog(LOG, "ivfflatbatchendscan: 调用MemoryContextDelete(so->base.tmpCtx) - tmpCtx: %p", so->base.tmpCtx);
    
    /* 检查tmpCtx指针有效性，只有有效时才删除 */
    /* tmpCtx应该是一个高地址的指针，高16位应该不为0 */
    /* 有效指针示例：0x5619fd8052a0，无效指针示例：0x300020000 */
    if (so->base.tmpCtx && ((uintptr_t)so->base.tmpCtx >> 32) > 0x1000) {
        elog(LOG, "ivfflatbatchendscan: tmpCtx指针有效，开始删除");
        MemoryContextDelete(so->base.tmpCtx);
        elog(LOG, "ivfflatbatchendscan: MemoryContextDelete(so->base.tmpCtx)完成");
    } else {
        elog(WARNING, "ivfflatbatchendscan: tmpCtx指针无效: %p，跳过删除", so->base.tmpCtx);
    }

    elog(LOG, "ivfflatbatchendscan: 调用pfree(so)");
    pfree(so);
    elog(LOG, "ivfflatbatchendscan: pfree(so)完成");
    
    scan->opaque = NULL;
    elog(LOG, "ivfflatbatchendscan: 函数执行完成");
}

#ifdef USE_CUDA
/*
 * 批量GPU聚类中心距离计算
 */
static void
GetScanLists_BatchGPU(IndexScanDesc scan, ScanKeyBatch batch_keys)
{
    elog(LOG, "GetScanLists_BatchGPU: 函数开始执行");
    
    elog(LOG, "GetScanLists_BatchGPU: 检查输入参数 - scan: %p, batch_keys: %p", scan, batch_keys);
    if (!scan) {
        elog(ERROR, "GetScanLists_BatchGPU: scan指针为空");
    }
    if (!batch_keys) {
        elog(ERROR, "GetScanLists_BatchGPU: batch_keys指针为空");
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 开始获取扫描状态");
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "GetScanLists_BatchGPU: 获取批量扫描状态成功, so=%p", so);
    
    if (!so) {
        elog(ERROR, "GetScanLists_BatchGPU: 批量扫描状态为空");
        return;
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 检查so结构体字段");
    if (!so->base.cuda_ctx) {
        elog(ERROR, "GetScanLists_BatchGPU: so->base.cuda_ctx为空");
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 批量查询数量=%d, 维度=%d, cuda_ctx=%p", 
         batch_keys->nkeys, so->base.dimensions, so->base.cuda_ctx);
    
    BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
    int totalLists = 0;
    int center_idx = 0;
    
    /* 检查聚类中心是否已上传到GPU，如果未上传则现在上传 */
    if (!so->base.centers_uploaded) {
        elog(LOG, "GetScanLists_BatchGPU: 聚类中心数据未上传，现在开始上传");
        if (UploadCentersToGPU_Batch(scan) != 0) {
            elog(ERROR, "GetScanLists_BatchGPU: 无法上传聚类中心数据到GPU，批量向量搜索需要GPU支持");
            return;
        }
        elog(LOG, "GetScanLists_BatchGPU: 聚类中心数据上传完成");
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 聚类中心数据已上传，开始计算距离");
    
    /* 计算总列表数量并收集列表页面信息 */
    elog(LOG, "GetScanLists_BatchGPU: 开始计算总列表数量并收集列表页面信息");
    BlockNumber *list_pages = NULL;
    
    while (BlockNumberIsValid(nextblkno))
    {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        totalLists += maxoffno;
        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 找到 %d 个列表，开始分配内存", totalLists);
    
    /* 分配内存存储列表页面信息 */
    list_pages = palloc(totalLists * sizeof(BlockNumber));
    if (!list_pages) {
        elog(ERROR, "GetScanLists_BatchGPU: 无法分配列表页面内存");
        return;
    }
    
    /* 收集列表页面信息 */
    elog(LOG, "GetScanLists_BatchGPU: 开始收集列表页面信息");
    nextblkno = IVFFLAT_HEAD_BLKNO;
    center_idx = 0;
    
    while (BlockNumberIsValid(nextblkno))
    {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);

        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
        {
            if (center_idx >= totalLists) {
                elog(ERROR, "GetScanLists_BatchGPU: 列表索引超出范围: %d >= %d", center_idx, totalLists);
                UnlockReleaseBuffer(cbuf);
                pfree(list_pages);
                return;
            }
            
            IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
            list_pages[center_idx] = list->startPage;
            center_idx++;
        }

        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    
    if (center_idx != totalLists) {
        elog(ERROR, "GetScanLists_BatchGPU: 列表数量不匹配: 预期 %d, 实际 %d", totalLists, center_idx);
        pfree(list_pages);
        return;
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 列表页面信息收集完成，开始准备批量查询数据");
    
    /* 准备批量查询向量数据 */
    elog(LOG, "GetScanLists_BatchGPU: 准备批量查询向量数据");
    float *batch_query_data = (float *) ScanKeyBatchGetContinuousData(batch_keys);
    int nbatch = batch_keys->nkeys;
    
    if (!batch_query_data) {
        elog(ERROR, "GetScanLists_BatchGPU: 无法获取批量查询数据");
        pfree(list_pages);
        return;
    }
    
    elog(LOG, "GetScanLists_BatchGPU: 批量查询数据准备完成，查询数量=%d", nbatch);
    
    /* 使用GPU批量计算距离 */
    elog(LOG, "GetScanLists_BatchGPU: 开始使用GPU批量计算距离");
    elog(LOG, "GetScanLists_BatchGPU: 参数 - cuda_ctx: %p, batch_query_data: %p, nbatch: %d, gpu_batch_distances: %p", 
         so->base.cuda_ctx, batch_query_data, nbatch, so->gpu_batch_distances);
    
    int cuda_result = cuda_compute_batch_center_distances((CudaCenterSearchContext*)so->base.cuda_ctx, 
                                                         batch_query_data, 
                                                         nbatch, 
                                                         so->gpu_batch_distances);
    elog(LOG, "GetScanLists_BatchGPU: CUDA批量距离计算结果: %d", cuda_result);
    
    if (cuda_result == 0) {
        elog(LOG, "GetScanLists_BatchGPU: GPU批量距离计算成功，开始处理结果");
        
        /* 为每个查询处理GPU计算结果 */
        for (int query_idx = 0; query_idx < nbatch; query_idx++) {
            int listCount = 0;
            double maxDistance = DBL_MAX;
            
            /* 重置列表队列 */
            pairingheap_reset(so->base.listQueue);
            
            /* 处理当前查询的距离结果 */
            elog(LOG, "GetScanLists_BatchGPU: 开始处理查询%d的距离结果, totalLists=%d, maxProbes=%d", 
                 query_idx, totalLists, so->base.maxProbes);
            for (int i = 0; i < totalLists; i++) {
                double distance = so->gpu_batch_distances[query_idx * totalLists + i];
                elog(LOG, "GetScanLists_BatchGPU: 聚类中心%d距离=%.6f, listCount=%d", i, distance, listCount);
                
                if (listCount < so->base.maxProbes) {
                    IvfflatScanList *scanlist = &so->base.lists[listCount];
                    scanlist->startPage = list_pages[i];
                    scanlist->distance = distance;
                    listCount++;
                    
                    /* Add to heap */
                    pairingheap_add(so->base.listQueue, &scanlist->ph_node);
                    elog(LOG, "GetScanLists_BatchGPU: 添加聚类中心%d到队列, listCount=%d", i, listCount);
                    
                    /* Calculate max distance */
                    if (listCount == so->base.maxProbes)
                        maxDistance = GetScanList(pairingheap_first(so->base.listQueue))->distance;
                }
                else if (distance < maxDistance) {
                    IvfflatScanList *scanlist = GetScanList(pairingheap_remove_first(so->base.listQueue));
                    
                    /* Reuse */
                    scanlist->startPage = list_pages[i];
                    scanlist->distance = distance;
                    pairingheap_add(so->base.listQueue, &scanlist->ph_node);
                    elog(LOG, "GetScanLists_BatchGPU: 替换聚类中心%d, 新距离=%.6f", i, distance);
                    
                    /* Update max distance */
                    maxDistance = GetScanList(pairingheap_first(so->base.listQueue))->distance;
                }
            }
            
            /* 输出排序结果到对应的查询 - 限制在maxProbes范围内 */
            int outputCount = Min(listCount, so->base.maxProbes);
            for (int i = outputCount - 1; i >= 0; i--) {
                so->base.listPages[i] = GetScanList(pairingheap_remove_first(so->base.listQueue))->startPage;
            }
            
            Assert(pairingheap_is_empty(so->base.listQueue));
        }
    } else {
        /* GPU计算失败，直接报错 */
        elog(ERROR, "批量向量搜索需要GPU支持，但GPU距离计算失败");
    }
    
    /* 清理临时内存 */
    pfree(list_pages);
}

/*
 * 批量GPU聚类中心数据上传
 */
static int
UploadCentersToGPU_Batch(IndexScanDesc scan)
{
    elog(LOG, "UploadCentersToGPU_Batch: 函数开始执行");
    
    elog(LOG, "UploadCentersToGPU_Batch: 检查输入参数 - scan: %p", scan);
    if (!scan) {
        elog(ERROR, "UploadCentersToGPU_Batch: scan指针为空");
        return -1;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: 开始获取扫描状态");
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "UploadCentersToGPU_Batch: 获取批量扫描状态成功, so=%p", so);
    
    if (!so) {
        elog(ERROR, "UploadCentersToGPU_Batch: 批量扫描状态为空");
        return -1;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: 检查so结构体字段");
    if (!so->base.cuda_ctx) {
        elog(ERROR, "UploadCentersToGPU_Batch: so->base.cuda_ctx为空");
        return -1;
    }
    
    /* 如果已经上传过，直接返回成功 */
    if (so->base.centers_uploaded) {
        elog(LOG, "UploadCentersToGPU_Batch: 聚类中心数据已上传，跳过");
        return 0;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: 维度=%d, cuda_ctx=%p, use_gpu=%s", 
         so->base.dimensions, so->base.cuda_ctx, so->base.use_gpu ? "是" : "否");
    
    BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
    int totalLists = 0;
    int center_idx = 0;
    int dimensions = so->base.dimensions;
    
    /* 检查CUDA上下文是否有效 */
    if (!so->base.cuda_ctx) {
        elog(ERROR, "UploadCentersToGPU_Batch: CUDA上下文为空，无法上传聚类中心数据");
        return -1;
    }
    
    
    elog(LOG, "UploadCentersToGPU_Batch: 开始收集聚类中心数据 (维度: %d)", dimensions);
    
    /* 计算总列表数量 */
    elog(LOG, "UploadCentersToGPU_Batch: 开始计算总列表数量");
    while (BlockNumberIsValid(nextblkno))
    {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);
        totalLists += maxoffno;
        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    
    if (totalLists <= 0) {
        elog(ERROR, "UploadCentersToGPU_Batch: 没有找到聚类中心数据");
        return -1;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: 找到 %d 个聚类中心，开始分配内存", totalLists);
    
    /* 分配内存存储聚类中心数据 */
    float *centers_data = palloc(totalLists * dimensions * sizeof(float));
    if (!centers_data) {
        elog(ERROR, "UploadCentersToGPU_Batch: 无法分配聚类中心数据内存 (%d个中心, %d维)", totalLists, dimensions);
        return -1;
    }
    
    /* 收集聚类中心数据 */
    elog(LOG, "UploadCentersToGPU_Batch: 开始收集聚类中心数据");
    nextblkno = IVFFLAT_HEAD_BLKNO;
    center_idx = 0;
    
    while (BlockNumberIsValid(nextblkno))
    {
        Buffer cbuf;
        Page cpage;
        OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
        LockBuffer(cbuf, BUFFER_LOCK_SHARE);
        cpage = BufferGetPage(cbuf);
        maxoffno = PageGetMaxOffsetNumber(cpage);

        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
        {
            if (center_idx >= totalLists) {
                elog(ERROR, "UploadCentersToGPU_Batch: 聚类中心索引超出范围: %d >= %d", center_idx, totalLists);
                UnlockReleaseBuffer(cbuf);
                pfree(centers_data);
                return -1;
            }
            
            IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
            
            /* 提取聚类中心向量数据 */
            Vector *center_vector = &list->center;
            float *center_data = &center_vector->x[0];  // 直接访问x数组
            
            /* 复制到centers_data数组 */
            memcpy(&centers_data[center_idx * dimensions], center_data, dimensions * sizeof(float));
            center_idx++;
        }

        nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;
        UnlockReleaseBuffer(cbuf);
    }
    
    if (center_idx != totalLists) {
        elog(ERROR, "UploadCentersToGPU_Batch: 聚类中心数量不匹配: 预期 %d, 实际 %d", totalLists, center_idx);
        pfree(centers_data);
        return -1;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: 聚类中心数据收集完成，开始上传到GPU");
    
    /* 上传聚类中心数据到GPU */
    int upload_result;
    CudaCenterSearchContext* ctx = (CudaCenterSearchContext*)so->base.cuda_ctx;
    
    /* 添加类型安全检查 */
    if (!ctx) {
        elog(ERROR, "UploadCentersToGPU_Batch: CUDA上下文指针为空");
        pfree(centers_data);
        return -1;
    }
    
    elog(LOG, "UploadCentersToGPU_Batch: CUDA上下文信息 - 聚类中心数: %d, 维度: %d, 零拷贝: %s, 已初始化: %s", 
         ctx->num_centers, ctx->dimensions, 
         ctx->use_zero_copy ? "是" : "否",
         ctx->initialized ? "是" : "否");
    
    if (!ctx->initialized) {
        elog(ERROR, "UploadCentersToGPU_Batch: CUDA上下文未初始化");
        pfree(centers_data);
        return -1;
    }
    
    if (ctx->use_zero_copy) {
        elog(LOG, "UploadCentersToGPU_Batch: 使用零拷贝模式上传数据");
        upload_result = cuda_upload_centers_zero_copy(ctx, centers_data);
        elog(LOG, "UploadCentersToGPU_Batch: 零拷贝上传结果: %d", upload_result);
    } else {
        elog(LOG, "UploadCentersToGPU_Batch: 使用标准模式上传数据");
        upload_result = cuda_upload_centers(ctx, centers_data);
        elog(LOG, "UploadCentersToGPU_Batch: 标准上传结果: %d", upload_result);
    }
    
    /* 清理临时内存 */
    pfree(centers_data);
    
    if (upload_result == 0) {
        so->base.centers_uploaded = true;
        elog(LOG, "UploadCentersToGPU_Batch: 批量聚类中心数据已成功上传到GPU (%d个中心)", totalLists);
    } else {
        elog(ERROR, "UploadCentersToGPU_Batch: 批量聚类中心数据上传到GPU失败，错误代码: %d", upload_result);
    }
    
    return upload_result;
}

/*
 * 批量GPU probes列表数据上传
 */
static int
UploadProbesToGPU_Batch(IndexScanDesc scan)
{
    elog(LOG, "UploadProbesToGPU_Batch: 函数开始执行");
    
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "UploadProbesToGPU_Batch: 获取批量扫描状态成功, so=%p", so);
    
    if (!so) {
        elog(ERROR, "UploadProbesToGPU_Batch: 批量扫描状态为空");
        return -1;
    }
    
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    int dimensions = so->base.dimensions;
    int totalProbesVectors = 0;
    int probesOffset = 0;
    
    elog(LOG, "UploadProbesToGPU_Batch: 维度=%d, maxProbes=%d", dimensions, so->base.maxProbes);
    
    /* 如果已经上传过，直接返回成功 */
    if (so->probes_uploaded) {
        elog(LOG, "UploadProbesToGPU_Batch: probes数据已上传，跳过重复上传");
        return 0;
    }
    
    elog(LOG, "UploadProbesToGPU_Batch: 开始计算总的probes向量数量");
    
    /* 计算总的probes向量数量 */
    for (int i = 0; i < so->base.maxProbes; i++) {
        BlockNumber searchPage = so->base.listPages[i];
        
        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->base.bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);
            
            totalProbesVectors += maxoffno;
            
            searchPage = IvfflatPageGetOpaque(page)->nextblkno;
            UnlockReleaseBuffer(buf);
        }
    }
    
    /* 分配内存存储probes向量数据 */
    float *probes_data = palloc(totalProbesVectors * dimensions * sizeof(float));
    int *probes_offsets = palloc(so->base.maxProbes * sizeof(int));
    int *probes_counts = palloc(so->base.maxProbes * sizeof(int));
    elog(LOG, "UploadProbesToGPU_Batch: 分配probes_tids内存 - totalProbesVectors: %d, 大小: %zu", 
         totalProbesVectors, totalProbesVectors * sizeof(ItemPointerData));
    ItemPointer *probes_tids = palloc(totalProbesVectors * sizeof(ItemPointer));
    elog(LOG, "UploadProbesToGPU_Batch: probes_tids分配完成，指针: %p", probes_tids);
    
    /* 收集probes向量数据 */
    for (int i = 0; i < so->base.maxProbes; i++) {
        BlockNumber searchPage = so->base.listPages[i];
        int listVectorCount = 0;
        
        probes_offsets[i] = probesOffset;
        
        while (BlockNumberIsValid(searchPage)) {
            Buffer buf;
            Page page;
            OffsetNumber maxoffno;

            buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->base.bas);
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            page = BufferGetPage(buf);
            maxoffno = PageGetMaxOffsetNumber(page);

            for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
                IndexTuple itup;
                Datum datum;
                bool isnull;
                ItemId itemid = PageGetItemId(page, offno);

                itup = (IndexTuple) PageGetItem(page, itemid);
                datum = index_getattr(itup, 1, tupdesc, &isnull);
                
                if (!isnull) {
                    Vector *vector = (Vector *) DatumGetPointer(datum);
                    float *vector_data = (float *) VARDATA(vector);
                    
                    /* 复制向量数据到probes_data数组 */
                    memcpy(&probes_data[probesOffset * dimensions], vector_data, dimensions * sizeof(float));
                    
                    /* 存储TID */
                    elog(LOG, "UploadProbesToGPU_Batch: 存储TID - probesOffset: %d, searchPage: %d, offno: %d", 
                         probesOffset, searchPage, offno);
                    probes_tids[probesOffset] = (ItemPointer)palloc(sizeof(ItemPointerData));
                    ItemPointerSet(probes_tids[probesOffset], searchPage, offno);
                    elog(LOG, "UploadProbesToGPU_Batch: TID存储完成 - 页号: %d, 偏移: %d", 
                         BlockIdGetBlockNumber(&probes_tids[probesOffset]->ip_blkid), 
                         ItemPointerGetOffsetNumber(probes_tids[probesOffset]));
                    
                    probesOffset++;
                    listVectorCount++;
                }
            }

            searchPage = IvfflatPageGetOpaque(page)->nextblkno;
            UnlockReleaseBuffer(buf);
        }
        
        probes_counts[i] = listVectorCount;
    }
    
    /* 上传probes数据到GPU */
    CudaCenterSearchContext* ctx = (CudaCenterSearchContext*)so->base.cuda_ctx;
    int upload_result = cuda_upload_probes_data(ctx, probes_data, probes_offsets, probes_counts, 
                                               so->base.maxProbes, totalProbesVectors, dimensions);
    
    /* 清理临时内存 */
    pfree(probes_data);
    pfree(probes_offsets);
    pfree(probes_counts);
    
    if (upload_result == 0) {
        so->probes_uploaded = true;
        elog(LOG, "UploadProbesToGPU_Batch: 存储probes_tids到扫描状态 - 指针: %p", probes_tids);
        so->probes_tids = probes_tids;
        so->total_probes_vectors = totalProbesVectors;
        elog(LOG, "UploadProbesToGPU_Batch: 扫描状态存储完成 - so->probes_tids: %p", so->probes_tids);
        elog(INFO, "批量probes列表数据已成功上传到GPU，共%d个向量", totalProbesVectors);
    } else {
        elog(LOG, "UploadProbesToGPU_Batch: GPU上传失败，释放probes_tids内存");
        pfree(probes_tids);
        elog(INFO, "批量probes列表数据上传到GPU失败");
    }
    
    return upload_result;
}

/*
 * 批量GPU扫描项目获取
 */
static void
GetScanItems_BatchGPU(IndexScanDesc scan, ScanKeyBatch batch_keys)
{
    elog(LOG, "GetScanItems_BatchGPU: 开始执行");
    
    elog(LOG, "GetScanItems_BatchGPU: 检查输入参数 - scan: %p, batch_keys: %p", scan, batch_keys);
    if (!scan) {
        elog(ERROR, "GetScanItems_BatchGPU: scan指针为空");
        return;
    }
    if (!batch_keys) {
        elog(ERROR, "GetScanItems_BatchGPU: batch_keys指针为空");
        return;
    }
    
    elog(LOG, "GetScanItems_BatchGPU: 开始获取扫描状态");
    IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
    elog(LOG, "GetScanItems_BatchGPU: 获取扫描状态成功, so=%p", so);
    
    if (!so) {
        elog(ERROR, "GetScanItems_BatchGPU: 扫描状态为空");
        return;
    }
    
    elog(LOG, "GetScanItems_BatchGPU: 获取关系描述符");
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    elog(LOG, "GetScanItems_BatchGPU: 关系描述符获取成功, tupdesc=%p", tupdesc);
    
    int nbatch = batch_keys->nkeys;
    int batchProbes = 0;

    elog(LOG, "GetScanItems_BatchGPU: 参数 - nbatch: %d, probes_uploaded: %s", 
         nbatch, so->probes_uploaded ? "是" : "否");

    /* 确保probes列表数据已上传到GPU */
    if (!so->probes_uploaded) {
        elog(LOG, "GetScanItems_BatchGPU: probes列表数据未上传，开始上传");
        if (UploadProbesToGPU_Batch(scan) != 0) {
            elog(ERROR, "批量向量搜索需要GPU支持，但probes列表数据上传失败");
        }
        elog(LOG, "GetScanItems_BatchGPU: probes列表数据上传完成");
    } else {
        elog(LOG, "GetScanItems_BatchGPU: probes列表数据已上传，跳过上传");
    }

    /* 为每个查询重置排序状态 */
    for (int i = 0; i < nbatch; i++) {
        tuplesort_reset(so->sortstates[i]);
    }

    /* 使用GPU批量计算probes距离 */
    if (so->probes_uploaded) {
        elog(LOG, "GetScanItems_BatchGPU: 开始使用GPU批量计算probes距离");
        
        /* 准备批量查询向量数据 */
        float *batch_query_data = (float *) ScanKeyBatchGetContinuousData(batch_keys);
        elog(LOG, "GetScanItems_BatchGPU: 批量查询数据准备完成, batch_query_data=%p", batch_query_data);
        
        /* 使用GPU批量计算probes距离 */
        CudaCenterSearchContext* ctx = (CudaCenterSearchContext*)so->base.cuda_ctx;
        elog(LOG, "GetScanItems_BatchGPU: 调用cuda_compute_batch_probes_distances, ctx=%p, nbatch=%d", ctx, nbatch);
        
        int probes_result = cuda_compute_batch_probes_distances(ctx, batch_query_data, nbatch, so->gpu_batch_distances);
        elog(LOG, "GetScanItems_BatchGPU: cuda_compute_batch_probes_distances结果: %d", probes_result);
        
        if (probes_result == 0) {
            elog(LOG, "GetScanItems_BatchGPU: GPU批量probes距离计算成功，开始处理结果");
            elog(LOG, "GetScanItems_BatchGPU: 参数 - nbatch: %d, total_probes_vectors: %d", nbatch, so->total_probes_vectors);
            
            /* 处理GPU计算结果并添加到排序状态 */
            for (int query_idx = 0; query_idx < nbatch; query_idx++) {
                Datum query_value = ScanKeyBatchGetVector(batch_keys, query_idx);
                
                elog(LOG, "GetScanItems_BatchGPU: 处理查询 %d，probes向量数量: %d", query_idx, so->total_probes_vectors);
                
                /* 为每个probes向量添加距离结果到排序状态 */
                for (int i = 0; i < so->total_probes_vectors; i++) {
                    float distance = so->gpu_batch_distances[query_idx * so->total_probes_vectors + i];
                    elog(LOG, "GetScanItems_BatchGPU: 查询 %d, probes %d, 距离: %f", query_idx, i, distance);
                    
                    /* 创建虚拟元组并添加到排序状态 */
                    ExecClearTuple(so->vslots[query_idx]);
                    so->vslots[query_idx]->tts_values[0] = Float8GetDatum(distance);
                    so->vslots[query_idx]->tts_isnull[0] = false;
                    
                    /* 设置真实的TID */
                    elog(LOG, "GetScanItems_BatchGPU: 准备设置TID - query_idx: %d, i: %d", query_idx, i);
                    elog(LOG, "GetScanItems_BatchGPU: so->probes_tids指针: %p", so->probes_tids);
                    if (so->probes_tids) {
                        elog(LOG, "GetScanItems_BatchGPU: 访问probes_tids[%d]", i);
                        ItemPointer tid = so->probes_tids[i];
                        elog(LOG, "GetScanItems_BatchGPU: 获取到TID指针: %p", tid);
                        if (tid) {
                            elog(LOG, "GetScanItems_BatchGPU: TID值 - 页号: %d, 偏移: %d", 
                                 BlockIdGetBlockNumber(&tid->ip_blkid), ItemPointerGetOffsetNumber(tid));
                        } else {
                            elog(LOG, "GetScanItems_BatchGPU: TID指针为空");
                        }
                        so->vslots[query_idx]->tts_values[1] = PointerGetDatum(tid);
                        so->vslots[query_idx]->tts_isnull[1] = false;
                        elog(LOG, "GetScanItems_BatchGPU: TID设置完成");
                    } else {
                        elog(LOG, "GetScanItems_BatchGPU: so->probes_tids为空，使用NULL");
                        so->vslots[query_idx]->tts_values[1] = PointerGetDatum(NULL);
                        so->vslots[query_idx]->tts_isnull[1] = true;
                    }
                    ExecStoreVirtualTuple(so->vslots[query_idx]);

                    tuplesort_puttupleslot(so->sortstates[query_idx], so->vslots[query_idx]);
                }
            }
        } else {
            elog(ERROR, "批量向量搜索需要GPU支持，但GPU probes距离计算失败");
        }
    } else {
        /* 回退到CPU计算 */
        elog(INFO, "probes数据未上传到GPU，回退到CPU计算");
        
        /* 搜索最近的probes列表 */
        while (so->base.listIndex < so->base.maxProbes && (++batchProbes) <= so->base.probes)
        {
            BlockNumber searchPage = so->base.listPages[so->base.listIndex++];

            /* 搜索列表的所有条目页面 */
            while (BlockNumberIsValid(searchPage))
            {
                Buffer buf;
                Page page;
                OffsetNumber maxoffno;

                buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->base.bas);
                LockBuffer(buf, BUFFER_LOCK_SHARE);
                page = BufferGetPage(buf);
                maxoffno = PageGetMaxOffsetNumber(page);

                for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
                {
                    IndexTuple itup;
                    Datum datum;
                    bool isnull;
                    ItemId itemid = PageGetItemId(page, offno);

                    itup = (IndexTuple) PageGetItem(page, itemid);
                    datum = index_getattr(itup, 1, tupdesc, &isnull);

                    /* 为每个查询计算距离并添加到对应的排序状态 */
                    for (int i = 0; i < nbatch; i++) {
                        Datum query_value = ScanKeyBatchGetVector(batch_keys, i);
                        
                        /* 计算距离 */
                        ExecClearTuple(so->vslots[i]);
                        so->vslots[i]->tts_values[0] = so->base.distfunc(so->base.procinfo, so->base.collation, datum, query_value);
                        so->vslots[i]->tts_isnull[0] = false;
                        so->vslots[i]->tts_values[1] = PointerGetDatum(&itup->t_tid);
                        so->vslots[i]->tts_isnull[1] = false;
                        ExecStoreVirtualTuple(so->vslots[i]);

                        tuplesort_puttupleslot(so->sortstates[i], so->vslots[i]);
                    }
                }

                searchPage = IvfflatPageGetOpaque(page)->nextblkno;

                UnlockReleaseBuffer(buf);
            }
        }
    }

    /* 对所有查询执行排序 */
    for (int i = 0; i < nbatch; i++) {
        tuplesort_performsort(so->sortstates[i]);
    }

#if defined(IVFFLAT_MEMORY)
    elog(INFO, "批量GPU内存: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}
#endif /* USE_CUDA */