#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

using namespace std;

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size): globalDepth(0),bucketsize(size),bucketsnum(1) {
    buckets.push_back(make_shared<Bucket>(0));

}

template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() :globalDepth(0),bucketsize(64),bucketsnum(1) {
    buckets.push_back(make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    size_t temp = hash<K>{}(key);
  return temp;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    lock_guard<mutex> lock(latch);
    return this->globalDepth;
}

template < typename K , typename V>
int ExtendibleHash<K,V>::getCnt(const K &key) {
    lock_guard<mutex> lck(latch);
    return HashKey(key) & ((1 << globalDepth) - 1);
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    if(this -> buckets[bucket_id])
    {
        lock_guard<mutex> lck(buckets[bucket_id]->latch);
        if (buckets[bucket_id]->bucketMap.size() == 0) return -1;
        return buckets[bucket_id]->localDepth;
    }
    return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    lock_guard<mutex> lock(latch);
    return this->bucketsnum;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    int temp = this->getCnt(key);
    lock_guard<mutex> lck(buckets[temp]->latch);
    shared_ptr<Bucket> current_bucket = this -> buckets[temp];
    if(current_bucket->bucketMap.find(key) != current_bucket->bucketMap.end())
    {
        value = current_bucket->bucketMap[key];
        return true;
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    int temp_cnt = this->getCnt(key);
    lock_guard<mutex> lck(buckets[temp_cnt]->latch);
    if (buckets[temp_cnt]->bucketMap.find(key) != buckets[temp_cnt]->bucketMap.end())
    {
        buckets[temp_cnt]->bucketMap.erase(key);
        return true;
    }
    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    int temp_cnt = this->getCnt(key);
    shared_ptr<Bucket> current_bucket = buckets[temp_cnt];
    while(true)
    {
        lock_guard<mutex> lck(current_bucket->latch);
        if(current_bucket->bucketMap.find(key) != current_bucket->bucketMap.end()
        || current_bucket->bucketMap.size() < this->bucketsize)
        {
            current_bucket->bucketMap[key] = value;
            break;
        }
        current_bucket->localDepth++;

        if(current_bucket-> localDepth > globalDepth)
        {
            int temp_size = this -> buckets.size();
            for(int i = 0; i < temp_size; i++)
            {
                this -> buckets.push_back(buckets[i]);
            }
            globalDepth++;
        }
        bucketsnum++;
        int getNewBit = 1<<((current_bucket->localDepth) - 1);
        shared_ptr<Bucket>  new_bucket = make_shared<Bucket>(current_bucket->localDepth);
        typename map<K, V>::iterator p;
        for(p = current_bucket->bucketMap.begin(); p != current_bucket->bucketMap.end();)
        {
            if(getNewBit & HashKey(p->first))
            {
                new_bucket->bucketMap[p->first] = p->second;
                p = current_bucket->bucketMap.erase(p);
            }else p++;
        }
        for (size_t i = 0; i < buckets.size(); i ++)
        {
            if(buckets[i] == current_bucket && (i & getNewBit))
            {
                buckets[i] = new_bucket;
            }
        }
        temp_cnt = getCnt(key);
        current_bucket = buckets[temp_cnt];
    }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
