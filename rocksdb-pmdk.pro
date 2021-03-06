TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += main.cpp \
    persistent_range_mem_set.cpp \
    persistent_chunk.cpp \
    fixed_range_tab.cpp

HEADERS += \
    pmdk-include/libpmemobj/action.h \
    pmdk-include/libpmemobj/action_base.h \
    pmdk-include/libpmemobj/atomic.h \
    pmdk-include/libpmemobj/atomic_base.h \
    pmdk-include/libpmemobj/base.h \
    pmdk-include/libpmemobj/ctl.h \
    pmdk-include/libpmemobj/iterator.h \
    pmdk-include/libpmemobj/iterator_base.h \
    pmdk-include/libpmemobj/lists_atomic.h \
    pmdk-include/libpmemobj/lists_atomic_base.h \
    pmdk-include/libpmemobj/pool.h \
    pmdk-include/libpmemobj/pool_base.h \
    pmdk-include/libpmemobj/thread.h \
    pmdk-include/libpmemobj/tx.h \
    pmdk-include/libpmemobj/tx_base.h \
    pmdk-include/libpmemobj/types.h \
    pmdk-include/libpmemobj++/detail/array_traits.hpp \
    pmdk-include/libpmemobj++/detail/check_persistent_ptr_array.hpp \
    pmdk-include/libpmemobj++/detail/common.hpp \
    pmdk-include/libpmemobj++/detail/conversions.hpp \
    pmdk-include/libpmemobj++/detail/integer_sequence.hpp \
    pmdk-include/libpmemobj++/detail/life.hpp \
    pmdk-include/libpmemobj++/detail/make_atomic_impl.hpp \
    pmdk-include/libpmemobj++/detail/persistent_ptr_base.hpp \
    pmdk-include/libpmemobj++/detail/pexceptions.hpp \
    pmdk-include/libpmemobj++/detail/specialization.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/array_traits.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/check_persistent_ptr_array.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/common.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/conversions.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/integer_sequence.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/life.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/make_atomic_impl.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/persistent_ptr_base.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/pexceptions.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/detail/specialization.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/allocator.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/condition_variable.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/make_persistent.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/make_persistent_array.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/make_persistent_array_atomic.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/make_persistent_atomic.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/mutex.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/p.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/persistent_ptr.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/pext.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/pool.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/shared_mutex.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/timed_mutex.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/transaction.hpp \
    pmdk-include/libpmemobj++/install/include/libpmemobj++/utils.hpp \
    pmdk-include/libpmemobj++/allocator.hpp \
    pmdk-include/libpmemobj++/condition_variable.hpp \
    pmdk-include/libpmemobj++/make_persistent.hpp \
    pmdk-include/libpmemobj++/make_persistent_array.hpp \
    pmdk-include/libpmemobj++/make_persistent_array_atomic.hpp \
    pmdk-include/libpmemobj++/make_persistent_atomic.hpp \
    pmdk-include/libpmemobj++/mutex.hpp \
    pmdk-include/libpmemobj++/p.hpp \
    pmdk-include/libpmemobj++/persistent_ptr.hpp \
    pmdk-include/libpmemobj++/pext.hpp \
    pmdk-include/libpmemobj++/pool.hpp \
    pmdk-include/libpmemobj++/shared_mutex.hpp \
    pmdk-include/libpmemobj++/timed_mutex.hpp \
    pmdk-include/libpmemobj++/transaction.hpp \
    pmdk-include/libpmemobj++/utils.hpp \
    pmdk-include/libpmem.h \
    pmdk-include/libpmemblk.h \
    pmdk-include/libpmemcto.h \
    pmdk-include/libpmemlog.h \
    pmdk-include/libpmemobj.h \
    pmdk-include/libpmempool.h \
    pmdk-include/libvmem.h \
    pmdk-include/libvmmalloc.h \
    persistent_range_mem_set.h \
    persistent_chunk.h \
    ex_common.h \
    fixed_range_chunk_based_nvm_write_cache.h \
    fixed_range_iterator.h \
    fixed_range_tab.h

INCLUDEPATH += pmdk-include
INCLUDEPATH += E:\github_repo\rocksdb\include
INCLUDEPATH += E:\github_repo\rocksdb
