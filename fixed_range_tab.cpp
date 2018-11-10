#include "fixed_range_tab.h"

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

///TODO：根对象是这样定义的吗
    struct my_root {
        size_t length; // mark end of chunk block sequence
        unsigned char data[MAX_BUF_LEN];
    };

    struct chunk_blk {
        unsigned char bloom_filter[CHUNK_BLOOM_FILTER_SIZE];
        size_t size;
        unsigned char data[];
    };

    FixedRangeTab::FixedRangeTab()
            : chunk_sum_size(0),
              MAX_CHUNK_SUM_SIZE(64 * 1024 * 1024),
              pop(nullptr) {

    }

    FixedRangeTab::~FixedRangeTab() {

    }

    InternalIterator *FixedRangeTab::NewInternalIterator(
            ColumnFamilyData *cfd, Arena *arena) {
        InternalIterator *internal_iter;
        TOID(my_root) rootp;

        MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(),
                                                arena);

        rootp = POBJ_ROOT(pop, my_root);

        char *dat = D_RO(rootp)->data;

        for (size_t chunkOffset : psttChunkList) {

        }


        for (PersistentChunk &psttChunk : psttChunkList) {
            merge_iter_builder.AddIterator(psttChunk.NewInternalIterator());
        }

        internal_iter = merge_iter_builder.Finish();
    }

    void FixedRangeTab::Append(const char *bloom_data,
                               const Slice &chunk_data,
                               const Slice &new_start,
                               const Slice &new_end) {
        if (chunk_sum_size + chunk_data.size_ >= MAX_CHUNK_SUM_SIZE) {
            ///TODO：chunk size的判断最后再做，当追加完成之后再检查是否超过size，允许超过chunk size
            // TODO
            // flush
            chunk_sum_size = 0;
        }

        // 开始追加
        ///TODO：考虑使用c++接口，更简单？
        TOID(my_root) rootp;
        TX_BEGIN(pop) {
                        /* TX_STAGE_WORK */
                        rootp = POBJ_ROOT(pop, my_root);
                        size_t cur_len = D_RO(rootp)->length;

                        // calculate total size
                        size_t chunk_blk_len = CHUNK_BLOOM_FILTER_SIZE + sizeof(chunk_data.size_)
                                               + chunk_data.size_;
                        // 添加持久化范围
                        TX_ADD_FIELD(rootp, length);
                        unsigned char *dest = D_RO(rootp)->data + cur_len;
                        pmemobj_tx_add_range_direct(dest, chunk_blk_len);
                        // 复制 chunk block
                        memcpy(dest, bloom_data, CHUNK_BLOOM_FILTER_SIZE);
                        memcpy(dest + CHUNK_BLOOM_FILTER_SIZE, chunk_data.size_, sizeof(chunk_data.size_));
                        dest += CHUNK_BLOOM_FILTER_SIZE + sizeof(chunk_data.size_);
                        memcpy(dest, chunk_data.data_, chunk_data.size_);
                        // 更新总长度
                                D_RW(rootp)->length = cur_len + chunk_blk_len;
                        chunk_sum_size += chunk_data.size_;
                        // chunk 坐标，即 chunk_blk.data
                        psttChunkList.push_back(dest);


//        dest += chunk_data.size_;
//        info.update(new_start, new_end);
//        memcpy(dest, info, sizeof)
//    } TX_ONCOMMIT {
//        /* TX_STAGE_ONCOMMIT */
//    } TX_ONABORT {
//        /* TX_STAGE_ONABORT */
//    } TX_FINALLY {
//        /* TX_STAGE_FINALLY */
        }TX_END
    }

    void FixedRange::Release() {
        ///TODO：chunk的空间释放
    }

    void FixedRangeTab::MaybeScheduleCompact() {

    }


} // namespace rocksdb
