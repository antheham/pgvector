/* scanbatch.c */
#include "postgres.h"
#include "access/genam.h"
#include "access/skey.h"
#include "utils/datum.h"
#include "access/relscan.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "utils/snapmgr.h"
#include "scanbatch.h"
#include "vector.h"
#include "vector_batch.h"
#include "ivfscanbatch.h"


// 函数声明
extern IndexScanDesc ivfflatbatchbeginscan(Relation index, int nkeys, int norderbys, ScanKeyBatch batch_keys);
extern void ivfflatbatchendscan(IndexScanDesc scan);

/*
 * 创建新的ScankeyBatch
 */
PG_FUNCTION_INFO_V1(scanbatch_create);
Datum
scanbatch_create(PG_FUNCTION_ARGS)
{
    int nkeys = PG_GETARG_INT32(0);
    int vec_dim = PG_GETARG_INT32(1);
    ScanKeyBatch batch = ScanKeyBatchCreate(nkeys, vec_dim);
    PG_RETURN_POINTER(batch);
}

PG_FUNCTION_INFO_V1(scanbatch_create_wrapper);
Datum
scanbatch_create_wrapper(PG_FUNCTION_ARGS)
{
    int nkeys = PG_GETARG_INT32(1);
    int vec_dim = PG_GETARG_INT32(2);
    ScanKeyBatch batch = ScanKeyBatchCreate(nkeys, vec_dim);
    PG_RETURN_POINTER(batch);
}

ScanKeyBatch
ScanKeyBatchCreate(int nkeys, int vec_dim)
{
    ScanKeyBatch batch;
    Size        size;
    Size        vec_size;

    /* 计算向量大小 */
    vec_size = VECTOR_SIZE(vec_dim);

    /* 分配ScankeyBatch结构 */
    batch = (ScanKeyBatch)palloc0(sizeof(ScanKeyBatchData));
    batch->nkeys = nkeys;
    batch->vec_dim = vec_dim;
    batch->vec_size = vec_size;

    /* 分配ScanKey数组 */
    batch->keys = (ScanKey)palloc(nkeys * sizeof(ScanKeyData));

    /* 分配连续内存存储所有向量数据 - 只存储浮点数部分 */
    size = nkeys * vec_dim * sizeof(float);
    batch->batch_data = palloc(size);
    batch->data_continuous = true;
    batch->keySize = sizeof(ScanKeyData);

    /* 初始化每个ScanKey */
    for (int i = 0; i < nkeys; i++)
    {
        ScanKey     key = &batch->keys[i];
        /* 修复：使用正确的偏移计算，batch_data只存储浮点数部分 */
        Pointer     vec_data = (Pointer)batch->batch_data + i * vec_dim * sizeof(float);

        /* 初始化ScanKey */
        key->sk_flags = 0;
        key->sk_attno = 1;
        key->sk_strategy = 0;
        key->sk_subtype = 0;
        key->sk_collation = 0;
        key->sk_func.fn_oid = InvalidOid;
        key->sk_argument = PointerGetDatum(vec_data);
    }

    return batch;
}

/*
 * 释放ScankeyBatch
 */
PG_FUNCTION_INFO_V1(scanbatch_free);
Datum
scanbatch_free(PG_FUNCTION_ARGS)
{
    ScanKeyBatch batch = (ScanKeyBatch)PG_GETARG_POINTER(0);
    ScanKeyBatchFree(batch);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(batch_vector_search_c);
Datum
batch_vector_search_c(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    BatchSearchState *state;
    
    elog(LOG, "batch_vector_search_c: 函数开始执行");
    
    if (SRF_IS_FIRSTCALL())
    {
        elog(LOG, "batch_vector_search_c: 第一次调用，初始化状态");
        
        Oid index_oid = PG_GETARG_OID(0);
        ArrayType *query_vectors_array = PG_GETARG_ARRAYTYPE_P(1);
        int k = PG_GETARG_INT32(2);
        
        elog(LOG, "batch_vector_search_c: 参数获取完成 - index_oid=%u, k=%d", index_oid, k);
        
        // 获取函数上下文
        funcctx = SRF_FIRSTCALL_INIT();
        elog(LOG, "batch_vector_search_c: SRF_FIRSTCALL_INIT 完成");
        
        // 分配状态结构
        state = (BatchSearchState *)MemoryContextAllocZero(funcctx->multi_call_memory_ctx, sizeof(BatchSearchState));
        if (!state) {
            elog(ERROR, "batch_vector_search_c: 内存分配失败");
        }
        elog(LOG, "batch_vector_search_c: 状态结构分配完成");
        funcctx->user_fctx = state;
        
        // 初始化状态
        state->index_oid = index_oid;
        state->query_vectors = query_vectors_array;
        state->k = k;
        state->current_query = 0;
        state->current_result = 0;
        elog(LOG, "batch_vector_search_c: 状态初始化完成");
        
        // 创建批量键
        int ndim = ARR_NDIM(query_vectors_array);
        int *dims = ARR_DIMS(query_vectors_array);
        int n_vectors = ArrayGetNItems(ndim, dims);
        elog(LOG, "batch_vector_search_c: 数组维度 = %d, dims[0] = %d, 向量数量 = %d", ndim, dims[0], n_vectors);
        
        // 检查数组结构
        elog(LOG, "batch_vector_search_c: ARR_NDIM = %d, ARR_DIMS[0] = %d", ARR_NDIM(query_vectors_array), ARR_DIMS(query_vectors_array)[0]);
        elog(LOG, "batch_vector_search_c: ARR_ELEMTYPE = %u", ARR_ELEMTYPE(query_vectors_array));
        
        // 获取第一个向量的维度
        Datum *elems;
        bool *nulls;
        int nelems;
        int16 typlen;
        bool typbyval;
        char typalign;
        elog(LOG, "batch_vector_search_c: 开始解析数组");
        get_typlenbyvalalign(ARR_ELEMTYPE(query_vectors_array), &typlen, &typbyval, &typalign);
        deconstruct_array(query_vectors_array, ARR_ELEMTYPE(query_vectors_array), typlen, typbyval, typalign, &elems, &nulls, &nelems);
        elog(LOG, "batch_vector_search_c: 数组解析完成，nelems=%d", nelems);
        
        int vec_dim = 3; // 默认维度
        if (nelems > 0 && !nulls[0]) {
            Vector *first_vec = DatumGetVector(elems[0]);
            vec_dim = first_vec->dim;
            elog(LOG, "batch_vector_search_c: 向量维度 = %d", vec_dim);
        } else {
            elog(LOG, "batch_vector_search_c: 使用默认维度 = %d", vec_dim);
        }
        
        elog(LOG, "batch_vector_search_c: 开始创建 ScanKeyBatch");
        
        // 在multi_call_memory_ctx中分配ScanKeyBatch，确保与state结构体在同一内存上下文中
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        state->batch_keys = ScanKeyBatchCreate(n_vectors, vec_dim);
        MemoryContextSwitchTo(oldcontext);
        
        if (!state->batch_keys) {
            elog(ERROR, "batch_vector_search_c: ScanKeyBatchCreate 失败");
        }
        elog(LOG, "batch_vector_search_c: ScanKeyBatch 创建完成 - batch_keys: %p, nkeys: %d, vec_dim: %d, vec_size: %zu", 
             state->batch_keys, state->batch_keys->nkeys, state->batch_keys->vec_dim, state->batch_keys->vec_size);
        
        // 添加查询向量到批量键
        elog(LOG, "batch_vector_search_c: 开始添加向量到批次");
        for (int i = 0; i < nelems; i++)
        {
            if (!nulls[i])
            {
                ScanKeyBatchAddVector(state->batch_keys, i, elems[i]);
                elog(LOG, "batch_vector_search_c: 添加向量 %d", i);
            }
        }
        elog(LOG, "batch_vector_search_c: 所有向量添加完成");
        
        // 创建批量扫描 - 在multi_call_memory_ctx中调用，确保内存上下文一致
        elog(LOG, "batch_vector_search_c: 开始创建索引扫描");
        state->index = index_open(index_oid, AccessShareLock);
        MemoryContext oldcontext2 = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        state->scan = ivfflatbatchbeginscan(state->index, 0, 1, state->batch_keys);
        MemoryContextSwitchTo(oldcontext2);
        if (!state->scan) {
            elog(ERROR, "batch_vector_search_c: ivfflatbatchbeginscan 失败");
        }
        elog(LOG, "batch_vector_search_c: 索引扫描创建完成");
        
        // 设置返回元组描述符
        if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");
        
        funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);
        elog(LOG, "batch_vector_search_c: 元组描述符准备完成");
    }
    
    elog(LOG, "batch_vector_search_c: 开始处理每次调用");
    funcctx = SRF_PERCALL_SETUP();
    state = (BatchSearchState *)funcctx->user_fctx;
    
    if (!state) {
        elog(ERROR, "batch_vector_search_c: 状态为空");
    }
    
    elog(LOG, "batch_vector_search_c: 检查batch_keys状态 - batch_keys: %p, nkeys: %d, vec_dim: %d, vec_size: %zu", 
         state->batch_keys, state->batch_keys ? state->batch_keys->nkeys : -1, 
         state->batch_keys ? state->batch_keys->vec_dim : -1, 
         state->batch_keys ? state->batch_keys->vec_size : -1);
    
    int total_queries = ArrayGetNItems(ARR_NDIM(state->query_vectors), ARR_DIMS(state->query_vectors));
    elog(LOG, "batch_vector_search_c: 当前查询 %d/%d", state->current_query, total_queries);
    
    
    // 使用实际的索引扫描获取结果
    elog(LOG, "batch_vector_search_c: 开始获取实际扫描结果，查询 %d/%d", state->current_query + 1, total_queries);
    
    // 准备存储结果的数组 - 获取所有查询的结果
    const int max_results = state->k * total_queries; // 每个查询k个结果 × 查询数量
    Datum *result_values = palloc(max_results * sizeof(Datum));
    bool *result_nulls = palloc(max_results * sizeof(bool));
    int returned_tuples = 0;
    
    // 调用实际的批量扫描函数
    elog(LOG, "batch_vector_search_c: 准备调用ivfflatbatchgettuple");
    elog(LOG, "batch_vector_search_c: 检查scan状态 - scan: %p, opaque: %p", 
         state->scan, state->scan ? state->scan->opaque : NULL);
    bool has_more = ivfflatbatchgettuple(state->scan, ForwardScanDirection, 
                                        result_values, result_nulls, 
                                        max_results, &returned_tuples, state->k);
    
    elog(LOG, "batch_vector_search_c: ivfflatbatchgettuple调用完成，has_more: %s, returned_tuples: %d", 
         has_more ? "true" : "false", returned_tuples);
    
    // 详细调试：分析返回的结果数据
    elog(LOG, "batch_vector_search_c: 调试信息 - max_results: %d, total_queries: %d, current_query: %d", 
         max_results, total_queries, state->current_query);
    
        // 检查返回的结果数据
        for (int debug_i = 0; debug_i < returned_tuples && debug_i < 5; debug_i++) {
            elog(LOG, "batch_vector_search_c: 结果 %d - isnull: %s", debug_i, result_nulls[debug_i] ? "true" : "false");
            if (!result_nulls[debug_i]) {
                // 获取元组槽位数据 - 模仿ivfscan.c的模式
                Datum *tuple_values = (Datum*)DatumGetPointer(result_values[debug_i]);
                bool *tuple_nulls = (bool*)(tuple_values + 2);  // 假设nulls数组跟在values后面
                
                // 第0列：距离值
                float8 distance = DatumGetFloat8(tuple_values[0]);
                
                // 第1列：TID指针
                ItemPointer heaptid = (ItemPointer)DatumGetPointer(tuple_values[1]);
                
                elog(LOG, "batch_vector_search_c: 结果 %d - 页号: %u, 偏移: %u, 距离: %.15f", 
                     debug_i, 
                     BlockIdGetBlockNumber(&heaptid->ip_blkid),
                     ItemPointerGetOffsetNumber(heaptid),
                     distance);
            }
        }
    elog(LOG, "batch_vector_search_c: 调用完成后检查scan状态 - scan: %p, opaque: %p", 
         state->scan, state->scan ? state->scan->opaque : NULL);
    
    if (!has_more || returned_tuples == 0) {
        // 当前查询没有更多结果，移动到下一个查询
        state->current_query++;
        pfree(result_values);
        pfree(result_nulls);
        
        // 检查是否所有查询都已完成
        if (state->current_query >= total_queries) {
            elog(LOG, "batch_vector_search_c: 所有查询完成，开始清理资源");
            // 清理资源
            if (state->scan) {
                elog(LOG, "batch_vector_search_c: 清理索引扫描");
                ivfflatbatchendscan(state->scan);
                state->scan = NULL; // 防止重复清理
            }
            if (state->index) {
                elog(LOG, "batch_vector_search_c: 关闭索引");
                index_close(state->index, AccessShareLock);
                state->index = NULL; // 防止重复清理
            }
            if (state->batch_keys) {
                elog(LOG, "batch_vector_search_c: 清理批次键");
                ScanKeyBatchFree(state->batch_keys);
                state->batch_keys = NULL; // 防止重复清理
            }
            elog(LOG, "batch_vector_search_c: 资源清理完成，返回 DONE");
            SRF_RETURN_DONE(funcctx);
        }
        
        // 递归调用以处理下一个查询
        return batch_vector_search_c(fcinfo);
    }
    
    // 处理返回的结果
    // ivfflatbatchgettuple 现在返回包含 tid 和 distance 的复合结构
    
    // 为当前查询创建结果数组
    typedef struct {
        ItemPointerData tid_data;  // 直接存储数据而不是指针
        float8 distance;
    } SearchResult;
    
    SearchResult *results = NULL;
    int valid_results = 0;
    
    // 只有在有结果时才分配内存
    if (returned_tuples > 0) {
        results = palloc(returned_tuples * sizeof(SearchResult));
    }
    
    // 从扫描结果中提取有效的结果
    for (int i = 0; i < returned_tuples; i++) {
        elog(LOG, "batch_vector_search_c: 处理结果 %d - scan: %p, opaque: %p", i, state->scan, state->scan ? state->scan->opaque : NULL);
        if (!result_nulls[i]) {
            // 获取元组槽位数据 - 模仿ivfscan.c的模式
            Datum *tuple_values = (Datum*)DatumGetPointer(result_values[i]);
            
            // 第0列：距离值
            float8 distance = DatumGetFloat8(tuple_values[0]);
            
            // 第1列：TID指针
            ItemPointer heaptid = (ItemPointer)DatumGetPointer(tuple_values[1]);
            
            elog(LOG, "batch_vector_search_c: 获取元组槽位数据 - scan: %p, opaque: %p", state->scan, state->scan ? state->scan->opaque : NULL);
            
            // 复制到结果数组
            results[valid_results].tid_data = *heaptid;
            results[valid_results].distance = distance;
            elog(LOG, "batch_vector_search_c: 复制完成 - scan: %p, opaque: %p", state->scan, state->scan ? state->scan->opaque : NULL);
            valid_results++;
        }
    }
    
    elog(LOG, "batch_vector_search_c: 找到 %d 个有效结果", valid_results);
    
    // 如果当前查询没有更多结果，移动到下一个查询
    if (valid_results == 0) {
        state->current_query++;
        
        // 清理当前查询的内存
        if (results) {
            pfree(results);
        }
        pfree(result_values);
        pfree(result_nulls);
        
        // 检查是否超出了查询数量
        if (state->current_query >= total_queries) {
            // 所有查询都已处理完成
            SRF_RETURN_DONE(funcctx);
        }
        
        // 重置当前结果计数器
        state->current_result = 0;
        
        // 重新开始处理下一个查询
        return batch_vector_search_c(fcinfo);
    }
    
    // 对结果按距离排序
    elog(LOG, "batch_vector_search_c: 开始对结果按距离排序");
    for (int i = 0; i < valid_results - 1; i++) {
        for (int j = i + 1; j < valid_results; j++) {
            if (results[i].distance > results[j].distance) {
                // 交换结果
                SearchResult temp = results[i];
                results[i] = results[j];
                results[j] = temp;
            }
        }
    }
    elog(LOG, "batch_vector_search_c: 结果排序完成");
    
    // 返回当前结果
    elog(LOG, "batch_vector_search_c: 准备返回结果 %d", state->current_result);
    Datum values[3];
    bool nulls[3] = {false, false, false};
    
    // 获取当前查询的结果
    int result_index = state->current_result % valid_results;
    
    // 检查results数组是否有效
    if (!results || valid_results == 0) {
        elog(ERROR, "batch_vector_search_c: results数组无效或为空");
    }
    
    // 从 ItemPointer 获取实际的表行ID
    ItemPointerData *tid_data = &results[result_index].tid_data;
    int32 vector_id = 0;
    
    // 通过ItemPointer访问堆表获取主键ID
    if (state->index) {
        Relation heap_rel = state->index->rd_index->indrelid ? 
                           relation_open(state->index->rd_index->indrelid, AccessShareLock) : NULL;
        
        if (heap_rel) {
            Buffer buffer;
            HeapTupleData heap_tuple;
            bool found = heap_fetch(heap_rel, SnapshotAny, &heap_tuple, &buffer, false);
            
            if (found) {
                // 假设主键是第一个属性（通常是id字段）
                TupleDesc tupdesc = RelationGetDescr(heap_rel);
                if (tupdesc->natts > 0) {
                    Datum id_datum = heap_getattr(&heap_tuple, 1, tupdesc, NULL);
                    vector_id = DatumGetInt32(id_datum);
                }
                ReleaseBuffer(buffer);
            }
            relation_close(heap_rel, AccessShareLock);
        }
    }
    
    // 如果无法获取主键ID，记录错误
    if (vector_id == 0) {
        elog(ERROR, "batch_vector_search_c: 无法获取主键ID，vector_id将为0");
    }
    
    values[0] = Int32GetDatum(state->current_query + 1);  // query_id
    values[1] = Int32GetDatum(vector_id);  // vector_id (表中存储向量的行的主键)
    values[2] = Float8GetDatum(results[result_index].distance);  // distance
    
    state->current_result++;
    
    // 如果当前查询的所有结果都已返回，移动到下一个查询
    if (state->current_result >= state->k) {
        state->current_query++;
        state->current_result = 0;
        
        // 清理当前查询的内存
        if (results) {
            pfree(results);
        }
        pfree(result_values);
        pfree(result_nulls);
        
        // 检查是否超出了查询数量
        if (state->current_query >= total_queries) {
            // 所有查询都已处理完成
            SRF_RETURN_DONE(funcctx);
        }
    }
    
    elog(LOG, "batch_vector_search_c: 创建元组");
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    if (!tuple) {
        elog(ERROR, "batch_vector_search_c: 元组创建失败");
    }
    
    elog(LOG, "batch_vector_search_c: 返回下一个结果");
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

void
ScanKeyBatchFree(ScanKeyBatch batch)
{
    if (batch == NULL)
        return;

    if (batch->keys != NULL)
        pfree(batch->keys);

    if (batch->batch_data != NULL)
        pfree(batch->batch_data);

    pfree(batch);
}

/*
 * 向ScankeyBatch添加向量
 */
PG_FUNCTION_INFO_V1(scanbatch_add_vector);
Datum
scanbatch_add_vector(PG_FUNCTION_ARGS)
{
    ScanKeyBatch batch = (ScanKeyBatch)PG_GETARG_POINTER(0);
    int index = PG_GETARG_INT32(1);
    Datum vector = PG_GETARG_DATUM(2);
    ScanKeyBatchAddVector(batch, index, vector);
    PG_RETURN_VOID();
}

void
ScanKeyBatchAddVector(ScanKeyBatch batch, int index, Datum vector)
{
    elog(LOG, "ScanKeyBatchAddVector: 开始添加向量，index=%d", index);
    
    if (index < 0 || index >= batch->nkeys)
        elog(ERROR, "index out of bounds in ScanKeyBatchAddVector");

    /* 获取目标内存位置 - 修复：使用正确的偏移计算 */
    Pointer     target = (Pointer)batch->batch_data + index * batch->vec_dim * sizeof(float);
    elog(LOG, "ScanKeyBatchAddVector: 目标位置计算完成，target=%p", target);
    
    Vector* src_vec = DatumGetVector(vector);
    elog(LOG, "ScanKeyBatchAddVector: 源向量维度 = %d, 期望维度 = %d", src_vec->dim, batch->vec_dim);
    elog(LOG, "ScanKeyBatchAddVector: 源向量地址 = %p, vl_len_ = %d", src_vec, src_vec->vl_len_);

    /* 检查维度是否匹配 */
    if (src_vec->dim != batch->vec_dim)
        elog(ERROR, "vector dimension mismatch in ScanKeyBatchAddVector: expected %d, got %d", batch->vec_dim, src_vec->dim);

    /* 打印源向量的前几个元素用于调试 */
    elog(LOG, "ScanKeyBatchAddVector: 源向量前5个元素:");
    for (int i = 0; i < Min(5, src_vec->dim); i++) {
        elog(LOG, "  src_vec->x[%d] = %f", i, src_vec->x[i]);
    }

    /* 复制向量数据到连续内存 - 直接访问x数组，它是灵活数组成员 */
    memcpy(target, &src_vec->x[0], batch->vec_dim * sizeof(float));
    elog(LOG, "ScanKeyBatchAddVector: 向量数据复制完成");

    /* 验证复制后的数据 */
    float* target_float = (float*)target;
    elog(LOG, "ScanKeyBatchAddVector: 复制后目标位置前5个元素:");
    for (int i = 0; i < Min(5, batch->vec_dim); i++) {
        elog(LOG, "  target[%d] = %f", i, target_float[i]);
    }

    /* 检查数据是否一致 */
    bool data_consistent = true;
    for (int i = 0; i < batch->vec_dim; i++) {
        if (src_vec->x[i] != target_float[i]) {
            data_consistent = false;
            elog(LOG, "ScanKeyBatchAddVector: 数据不一致 at index %d: src=%f, target=%f", 
                 i, src_vec->x[i], target_float[i]);
            break;
        }
    }
    elog(LOG, "ScanKeyBatchAddVector: 数据一致性检查: %s", data_consistent ? "通过" : "失败");
}

/*
 * 从ScankeyBatch获取向量
 */
Datum
ScanKeyBatchGetVector(ScanKeyBatch batch, int index)
{
    if (index < 0 || index >= batch->nkeys)
        elog(ERROR, "index out of bounds in ScanKeyBatchGetVector");

    /* 创建Vector结构体，包含vl_len_字段 */
    Vector *vec = (Vector *)palloc(VECTOR_SIZE(batch->vec_dim));
    SET_VARSIZE(vec, VECTOR_SIZE(batch->vec_dim));
    vec->dim = batch->vec_dim;
    vec->unused = 0;
    
    /* 复制浮点数数据 */
    float *vec_data = (float *)batch->batch_data + index * batch->vec_dim;
    memcpy(vec->x, vec_data, batch->vec_dim * sizeof(float));
    
    return PointerGetDatum(vec);
}

/*
 * 检查数据是否连续存储
 */
bool
ScanKeyBatchIsContinuous(ScanKeyBatch batch)
{
    return batch->data_continuous;
}

/*
 * 获取连续存储的数据指针
 */
void*
ScanKeyBatchGetContinuousData(ScanKeyBatch batch)
{
    elog(LOG, "ScanKeyBatchGetContinuousData: 返回batch_data=%p, nkeys=%d, vec_dim=%d", 
         batch->batch_data, batch->nkeys, batch->vec_dim);
    
    /* 打印前几个向量的前几个元素用于调试 */
    float* data = (float*)batch->batch_data;
    for (int i = 0; i < Min(2, batch->nkeys); i++) {
        elog(LOG, "ScanKeyBatchGetContinuousData: 向量%d前5个元素:", i);
        for (int j = 0; j < Min(5, batch->vec_dim); j++) {
            int idx = i * batch->vec_dim + j;
            elog(LOG, "  data[%d] = %f", idx, data[idx]);
        }
    }
    
    return batch->batch_data;
}