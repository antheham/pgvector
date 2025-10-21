#include "postgres.h"

#include <float.h>

#include "access/amapi.h"
#include "access/reloptions.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "ivfflat.h"
#include "ivfscanbatch.h"
#include "scanbatch.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"


#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int			ivfflat_probes;
int			ivfflat_iterative_scan;
int			ivfflat_max_probes;
static relopt_kind ivfflat_relopt_kind;

static const struct config_enum_entry ivfflat_iterative_scan_options[] = {
	{"off", IVFFLAT_ITERATIVE_SCAN_OFF, false},
	{"relaxed_order", IVFFLAT_ITERATIVE_SCAN_RELAXED, false},
	{NULL, 0, false}
};

/*
 * Initialize index options and variables
 */
void
IvfflatInit(void)
{
	ivfflat_relopt_kind = add_reloption_kind();
	add_int_reloption(ivfflat_relopt_kind, "lists", "Number of inverted lists",
					  IVFFLAT_DEFAULT_LISTS, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, AccessExclusiveLock);

	DefineCustomIntVariable("ivfflat.probes", "Sets the number of probes",
							"Valid range is 1..lists.", &ivfflat_probes,
							IVFFLAT_DEFAULT_PROBES, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable("ivfflat.iterative_scan", "Sets the mode for iterative scans",
							 NULL, &ivfflat_iterative_scan,
							 IVFFLAT_ITERATIVE_SCAN_OFF, ivfflat_iterative_scan_options, PGC_USERSET, 0, NULL, NULL, NULL);

	/* If this is less than probes, probes is used */
	DefineCustomIntVariable("ivfflat.max_probes", "Sets the max number of probes for iterative scans",
							NULL, &ivfflat_max_probes,
							IVFFLAT_MAX_LISTS, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);

#ifdef USE_CUDA
	// 验证cuda代码是否被正确编译
	if(!cuda_is_available()){
		elog(LOG, "CUDA is not available, GPU features will be disabled!");
	} else {
		elog(LOG, "CUDA is available, GPU features are enabled!");
	}
#endif
	MarkGUCPrefixReserved("ivfflat");
}

/*
 * Get the name of index build phase
 */
static char *
ivfflatbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_IVFFLAT_PHASE_KMEANS:
			return "performing k-means";
		case PROGRESS_IVFFLAT_PHASE_ASSIGN:
			return "assigning tuples";
		case PROGRESS_IVFFLAT_PHASE_LOAD:
			return "loading tuples";
		default:
			return NULL;
	}
}

/*
 * Estimate the cost of an index scan
 */
static void
ivfflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
	GenericCosts costs;
	int			lists;
	double		ratio;
	double		sequentialRatio = 0.5;
	double		startupPages;
	double		spc_seq_page_cost;
	Relation	index;

	/* Never use index without order */
	if (path->indexorderbys == NULL)
	{
		*indexStartupCost = get_float8_infinity();
		*indexTotalCost = get_float8_infinity();
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
#if PG_VERSION_NUM >= 180000
		/* See "On disable_cost" thread on pgsql-hackers */
		path->path.disabled_nodes = 2;
#endif
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

	genericcostestimate(root, path, loop_count, &costs);

	index = index_open(path->indexinfo->indexoid, NoLock);
	IvfflatGetMetaPageInfo(index, &lists, NULL);
	index_close(index, NoLock);

	/* Get the ratio of lists that we need to visit */
	ratio = ((double) ivfflat_probes) / lists;
	if (ratio > 1.0)
		ratio = 1.0;

	get_tablespace_page_costs(path->indexinfo->reltablespace, NULL, &spc_seq_page_cost);

	/* Change some page cost from random to sequential */
	costs.indexTotalCost -= sequentialRatio * costs.numIndexPages * (costs.spc_random_page_cost - spc_seq_page_cost);

	/* Startup cost is cost before returning the first row */
	costs.indexStartupCost = costs.indexTotalCost * ratio;

	/* Adjust cost if needed since TOAST not included in seq scan cost */
	startupPages = costs.numIndexPages * ratio;
	if (startupPages > path->indexinfo->rel->pages && ratio < 0.5)
	{
		/* Change rest of page cost from random to sequential */
		costs.indexStartupCost -= (1 - sequentialRatio) * startupPages * (costs.spc_random_page_cost - spc_seq_page_cost);

		/* Remove cost of extra pages */
		costs.indexStartupCost -= (startupPages - path->indexinfo->rel->pages) * spc_seq_page_cost;
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
ivfflatoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  ivfflat_relopt_kind,
									  sizeof(IvfflatOptions),
									  tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
ivfflatvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflathandler);
Datum
ivfflathandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 5;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
	amroutine->amcanbuildparallel = true;
#endif
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = ivfflatbuild;
	amroutine->ambuildempty = ivfflatbuildempty;
	amroutine->aminsert = ivfflatinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = ivfflatbulkdelete;
	amroutine->amvacuumcleanup = ivfflatvacuumcleanup;
	amroutine->amcanreturn = NULL;	/* tuple not included in heapsort */
	amroutine->amcostestimate = ivfflatcostestimate;
	amroutine->amoptions = ivfflatoptions;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
	amroutine->ambuildphasename = ivfflatbuildphasename;
	amroutine->amvalidate = ivfflatvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = ivfflatbeginscan;
	amroutine->amrescan = ivfflatrescan;
	amroutine->amgettuple = ivfflatgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = ivfflatendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * 创建批量扫描描述符
 * 这是一个自定义函数，用于支持批量向量查询
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_batch_scan_create);
Datum
ivfflat_batch_scan_create(PG_FUNCTION_ARGS)
{
	Oid index_oid = PG_GETARG_OID(0);
	Relation index = index_open(index_oid, AccessShareLock);
	int nkeys = PG_GETARG_INT32(1);
	int norderbys = PG_GETARG_INT32(2);
	ScanKeyBatch batch_keys = (ScanKeyBatch)PG_GETARG_POINTER(3);
	
	IndexScanDesc scan = ivfflatbatchbeginscan(index, nkeys, norderbys, batch_keys);
	
	PG_RETURN_POINTER(scan);
}


/*
 * 获取批量扫描结果（包含距离信息）
 * 这是一个自定义函数，用于获取批量向量查询结果和距离
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_batch_scan_get_results);
Datum
ivfflat_batch_scan_get_results(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
	ScanDirection dir = PG_GETARG_INT32(1);
	int k = PG_GETARG_INT32(2);  // 每个查询的k值（用户指定的LIMIT值）
	
	/* 调用批量扫描核心函数获取结果 */
	IvfflatBatchScanOpaque so = (IvfflatBatchScanOpaque)scan->opaque;
	int nbatch = so->batch_keys->nkeys;
	int total_results = 0;
	int max_tuples = k * nbatch;  // 总的最大元组数 = 每个查询的k值 × 查询数量
	
	/* 创建结果数组 */
	Datum *tids = (Datum *)palloc(max_tuples * sizeof(Datum));
	float8 *distances = (float8 *)palloc(max_tuples * sizeof(float8));
	int *query_ids = (int *)palloc(max_tuples * sizeof(int));
	int *result_ids = (int *)palloc(max_tuples * sizeof(int));
	
	/* 处理每个查询的结果 */
	for (int query_idx = 0; query_idx < nbatch; query_idx++) {
		Tuplesortstate* sortstate = so->sortstates[query_idx];
		TupleTableSlot* mslot = so->mslots[query_idx];
		ItemPointer heaptid;
		bool isnull_local;
		int count = 0;
		
		/* 从排序状态获取元组 */
		while (count < k && tuplesort_gettupleslot(sortstate, true, false, mslot, NULL)) {
			float8 distance = DatumGetFloat8(slot_getattr(mslot, 1, &isnull_local));
			heaptid = (ItemPointer)DatumGetPointer(slot_getattr(mslot, 2, &isnull_local));
			
			if (!isnull_local && heaptid != NULL) {
				tids[total_results] = PointerGetDatum(heaptid);
				distances[total_results] = distance;
				query_ids[total_results] = query_idx + 1;  // query_id从1开始
				result_ids[total_results] = count + 1;     // result_id从1开始
				total_results++;
				count++;
			}
		}
	}
	
	/* 创建结果数组 */
	ArrayType *tid_array;
	tid_array = construct_array(tids, total_results, TIDOID, sizeof(ItemPointer), true, 'i');
	ArrayType *distance_array = construct_array((Datum*)distances, total_results, FLOAT8OID, sizeof(float8), true, 'd');
	ArrayType *query_id_array = construct_array((Datum*)query_ids, total_results, INT4OID, sizeof(int32), true, 'i');
	ArrayType *result_id_array = construct_array((Datum*)result_ids, total_results, INT4OID, sizeof(int32), true, 'i');
	
	/* 创建复合类型结果 */
	TupleDesc tupdesc = CreateTemplateTupleDesc(4);
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "heap_tid", TIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber)2, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber)3, "query_id", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber)4, "result_id", INT4OID, -1, 0);
	
	Datum values[4];
	bool nulls[4] = {false, false, false, false};
	
	values[0] = PointerGetDatum(tid_array);
	values[1] = PointerGetDatum(distance_array);
	values[2] = PointerGetDatum(query_id_array);
	values[3] = PointerGetDatum(result_id_array);
	
	HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
	
	/* 清理内存 */
	pfree(tids);
	pfree(distances);
	pfree(query_ids);
	pfree(result_ids);
	
	PG_RETURN_POINTER(tuple);
}

/*
 * 结束批量扫描
 * 这是一个自定义函数，用于清理批量向量查询资源
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_batch_scan_end);
Datum
ivfflat_batch_scan_end(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
	
	ivfflatbatchendscan(scan);
	
	PG_RETURN_NULL();
}
