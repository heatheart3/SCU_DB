/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <mutex>
#include <memory>

#include "hash/hash_table.h"

using namespace std;

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash();
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  int getCnt(const K &key);
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  struct Bucket
    {
      Bucket(int depth)
      {
          localDepth = depth;
      }
      int localDepth;
      map<K, V> bucketMap;
      mutex latch;
    };
private:
    int globalDepth;
    size_t bucketsize;
    vector<shared_ptr<Bucket>> buckets;
    int bucketsnum;
    mutable mutex latch;
};
} // namespace scudb
