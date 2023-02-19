/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define INDEXITERATOR_TYPE \
  IndexIterator<KeyType, ValueType, KeyComparator>

    INDEX_TEMPLATE_ARGUMENTS
    class IndexIterator {
    public:
        // you may define your own constructor based on your member variables
        IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bpm);

        virtual ~IndexIterator();

        bool isEnd();

        const MappingType &operator*();

        IndexIterator &operator++();

    private:
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
        int index_;
        BufferPoolManager *buff_pool_manager_;
    };

} // namespace scudb
