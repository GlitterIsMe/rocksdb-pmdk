#ifndef PERSISTENT_RANGE_MEM_SET_H
#define PERSISTENT_RANGE_MEM_SET_H

#include <rocksdb/iterator.h>

#include <fixed_range_tab.h>
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/p.hpp"

namespace rocksdb {

    class PersistentRangeMemSet {
    public:
        PersistentRangeMemSet(const std::string& path);

        ~PersistentRangeMemSet();

    public:
        // 返回所有RangeMemtable的有序序列
        // 基于MergeIterator
        Iterator *NewIterator();

        // 获取range_mem_id对应的RangeMemtable结构
        FixedRangeTab *GetRangeMemtable(uint64_t range_mem_id);

        // 根据key查找value
        Status Get(const Slice &key, std::string *value);

        FixedRangeTab* NewRangeTab(const std::string& prefix);

    private:
        //  persistent_queue<FixedRangeTab> range_mem_list_;
        //persistent_map <range, FixedRangeTab> range2tab;

        void Init();
        pmem::obj::persistent_ptr<persistent_map<std::string, FixedRangeTab>> pmap_;
        //  persistent_queue<uint64_t> compact_queue_;

        //PMEMobjpool *pop;

        pmem::obj::pool<persistent_map<std::string, FixedRangeTab> > pop;

        pmem::obj ::p<bool> inited_ = false;
    };

} // namespace rocksdb
#endif // PERSISTENT_RANGE_MEM_SET_H
