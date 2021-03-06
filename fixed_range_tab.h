#ifndef PERSISTENT_RANGE_MEM_H
#define PERSISTENT_RANGE_MEM_H

#include <list>

#include <db/db_impl.h>
//#include <rocksdb/slice.h>
//#include <rocksdb/iterator.h>
//#include <table/merging_iterator.h>

#include <libpmemobj.h>

#include <persistent_chunk.h>
#include <pmdk-include/libpmemobj++/pool.hpp>

namespace rocksdb {

using std::list;

struct FixedRangeChunkBasedCacheStats {
    uint64_t  used_bits_;
//    std::unordered_map<std::string, uint64_t> range_list_;
//    std::vector<std::string*> chunk_bloom_data_;

    // 预设的range
    Slice start, end;
};

class freqUpdateInfo {
public:
    explicit freqUpdateInfo(const Slice& real_start, const Slice& real_end)
        :real_start_(real_start), real_end_(real_end) {

    }
    void update(const Slice& real_start, const Slice& real_end) {
        if (real_start.compare(real_start_) < 0)
            real_start_ = real_start;
        if (real_end.compare(real_end_) > 0)
            real_end_ = real_end;
    }

    // 实际的range
    // TODO 初始值怎么定
    Slice real_start_;
    Slice real_end_;
    size_t chunk_num;
};

class FixedRangeTab
{
public:
    FixedRangeTab(pmem::obj::pool_base& pop;);
    ~FixedRangeTab();

public:
    // 返回当前RangeMemtable中所哟chunk的有序序列
    // 基于MergeIterator
    // 参考 DBImpl::NewInternalIterator
    InternalIterator* NewInternalIterator(ColumnFamilyData* cfd, Arena* arena);
    Status Get(const Slice& key, std::string *value);

    // 返回当前RangeMemtable是否正在被compact
    bool IsCompactWorking();

    // 返回当前RangeMemtable的Global Bloom Filter
//    char* GetBloomFilter();

    // 将新的chunk数据添加到RangeMemtable
    void Append(const char *bloom_data, const Slice& chunk_data,
                const Slice& new_start, const Slice& new_end);

    // 更新当前RangeMemtable的Global Bloom Filter
//    void SetBloomFilter(char* bloom_data);

    // 返回当前RangeMem的真实key range（stat里面记录）
    void GetRealRange(Slice& real_start, Slice& real_end) {
        real_start = info.real_start_;
        real_end = info.real_end_;
    }

    // 更新当前RangeMemtable的状态
    void UpdateStat(const Slice& new_start, const Slice& new_end);

    // 判断是否需要compact，如果需要则将一个RangeMemid加入Compact队列
    void MaybeScheduleCompact();

    // 释放当前RangeMemtable的所有chunk以及占用的空间
    void Release();

    // 重置Stat数据以及bloom filter
    void CleanUp();

private:
    FixedRangeTab(const FixedRangeTab&) = delete;
    FixedRangeTab& operator=(const FixedRangeTab&) = delete;

    pmem::obj::pool_base& pop_;

    FixedRangeChunkBasedCacheStats stat;
    freqUpdateInfo info;

    unsigned int memid;
    std::string file_path;

    size_t chunk_sum_size;
    const size_t MAX_CHUNK_SUM_SIZE;

    // offset of all chunks are stored in this list
    // TODO：直接使用list结构好像不能持久化,如何恢复？
    list<size_t> psttChunkList;

    bool is_compaction_working;


//    char *g_bloom_data;
};

} // namespace rocksdb

#endif // PERSISTENT_RANGE_MEM_H
