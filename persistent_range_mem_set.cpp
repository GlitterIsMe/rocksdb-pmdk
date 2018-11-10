#include "persistent_range_mem_set.h"

#include <ex_common.h>
#include <pmdk-include/libpmemobj++/make_persistent.hpp>

namespace rocksdb {

    PersistentRangeMemSet::PersistentRangeMemSet(const std::string path) {
        //    file_path.data()
        if (file_exists(path.c_str() != 0) {
            pop = pmem::obj::pool<PersistentRangeMemSet>::create(path, "range_based_cache", static_cast<uint64_t>(40) * 1024 * 1024 * 1024, CREATE_MODE_RW);
        } else {
            pop = pmem::obj::pool<PersistentRangeMemSet>::open(path, "range_based_cache");
        }

        pmap_ = pop.root();
    }

    void PersistentRangeMemSet::Init() {
        if(!inited_){
            pmem::obj::transcation::run(pop, [&]{
                pmap_ = pmem::obj::make_persistent<persistent_map<range, FixedRangeTab> >();
            });
            inited_ = true;
        }
    }

    PersistentRangeMemSet::~PersistentRangeMemSet() {
        pop.close();
    }

    FixedRangeTab* PersistentRangeMemSet::GetRangeMemtable(uint64_t range_mem_id) {

    }

    Status PersistentRangeMemSet::Get(const Slice &key, std::string *value) {

    }

    FixedRangeTab* PersistentRangeMemSet::NewRangeTab(const std::string &prefix) {

    }

} // namespace rocksdb


