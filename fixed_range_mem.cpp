#include "fixed_range_mem.h"

#include <ex_common.h>

#include <table/merging_iterator.h>

#include <persistent_chunk.h>

namespace rocksdb {

#define MAX_BUF_LEN 4096
#define CHUNK_BLOOM_FILTER_SIZE 8

POBJ_LAYOUT_BEGIN(range_mem);
POBJ_LAYOUT_ROOT(range_mem, struct my_root);
POBJ_LAYOUT_TOID(range_mem, struct foo_el);
POBJ_LAYOUT_TOID(range_mem, struct bar_el);
POBJ_LAYOUT_END(range_mem);

struct my_root {
    size_t length;
    unsigned char data[MAX_BUF_LEN];
};

struct chunk_blk {
    unsigned char bloom_filter[CHUNK_BLOOM_FILTER_SIZE];
    size_t size;
    unsigned char data[];
};

FixedRange::FixedRange()
    : chunk_sum_size(0),
      MAX_CHUNK_SUM_SIZE(64 * 1024 * 1024),
      pop(nullptr)
{
    //    file_path.data()
    if (file_exists(path) != 0) {
        if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(range_mem),
                                  PMEMOBJ_MIN_POOL, 0666)) == NULL) {
            perror("failed to create pool\n");
            return 1;
        }
    } else {
        if ((pop = pmemobj_open(path,
                                POBJ_LAYOUT_NAME(range_mem))) == NULL) {
            perror("failed to open pool\n");
            return 1;
        }
    }
}

FixedRange::~FixedRange()
{
    if (pop)
        pmemobj_close(pop);
}

InternalIterator* FixedRange::NewInternalIterator(
        ColumnFamilyData *cfd, Arena *arena)
{
    InternalIterator* internal_iter;
    TOID(my_root) rootp;

    MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(),
                                            arena);

    rootp = POBJ_ROOT(pop, my_root);

    for (size_t chunkOffset : psttChunkList) {

    }


    for (PersistentChunk &psttChunk : psttChunkList) {
        merge_iter_builder.AddIterator(psttChunk.NewInternalIterator());
    }

    internal_iter = merge_iter_builder.Finish();
}

void FixedRange::Append(const char *bloom_data,
                                         const Slice &chunk_data,
                                         const Slice &new_start,
                                         const Slice &new_end)
{
    if (chunk_sum_size + chunk_data.size_ >= MAX_CHUNK_SUM_SIZE) {
        // TODO
        // flush
        chunk_sum_size = 0;
    }

    // 开始追加
    TOID(my_root) rootp;
    TX_BEGIN(pop) {
        /* TX_STAGE_WORK */
        rootp = POBJ_ROOT(pop, my_root);
        size_t cur_len = D_RO(rootp)->length;
        size_t chunk_blk_len = CHUNK_BLOOM_FILTER_SIZE + sizeof(chunk_data.size_)
                + chunk_data.size_;
        // 添加持久化范围
        TX_ADD_FIELD(rootp, length);
        unsigned char *dest = D_RO(rootp)->data + cur_len;
        pmemobj_tx_add_range_direct(dest, chunk_blk_len);
        // 复制 chunk block
        memcpy(dest, bloom_data, CHUNK_BLOOM_FILTER_SIZE);
        memcpy(dest+CHUNK_BLOOM_FILTER_SIZE, chunk_data.size_, sizeof(chunk_data.size_));
        dest += CHUNK_BLOOM_FILTER_SIZE+sizeof(chunk_data.size_);
        memcpy(dest, chunk_data.data_, chunk_data.size_);
        // 更新总长度
        D_RW(rootp)->length = cur_len + chunk_blk_len;
        chunk_sum_size += chunk_data.size_;
        // chunk 坐标，即 chunk_blk.data
        psttChunkList.push_back(dest);
//    } TX_ONCOMMIT {
//        /* TX_STAGE_ONCOMMIT */
//    } TX_ONABORT {
//        /* TX_STAGE_ONABORT */
//    } TX_FINALLY {
//        /* TX_STAGE_FINALLY */
    } TX_END
}

void FixedRange::Release()
{

}

} // namespace rocksdb
