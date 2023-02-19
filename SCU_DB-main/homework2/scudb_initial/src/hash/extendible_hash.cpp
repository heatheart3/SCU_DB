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
//根据测试代码要求，设置自动构建默认桶大小为64
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() :globalDepth(0),bucketsize(64),bucketsnum(1) {
    buckets.push_back(make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
//返回一个哈希值回去
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
//桶在buckets这个动态数组中排列，这里根据全局深度使用按位运算，取出当前深度下对应位的数值，返回桶序号
template < typename K , typename V>
int ExtendibleHash<K,V>::getCnt(const K &key) {
    lock_guard<mutex> lck(latch);
    return HashKey(key) & ((1 << globalDepth) - 1);
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
//获取特定序号的桶的局部深度
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    if(this -> buckets[bucket_id]) //因为vector使用下标访问可能越界，因此这里要进行一次判断
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
//返回桶数量。不使用vector的size函数而单独设计的原因是在全局深度增加时，vector内的索引的数量会翻倍，此时vector的size不等于桶数，所以要单独设计该函数
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
        //可以插入有两种情况：桶没有满或者桶满了但是这个实际有相同key值的键值对，那么第二种情况也可以插入，只需要更新键的对应值即可
        {
            current_bucket->bucketMap[key] = value;
            break;
        }
        //如果不能插入，那么局部深度一定需要增加，所以直接++
        current_bucket->localDepth++;

        if(current_bucket-> localDepth > globalDepth)
        {
            int temp_size = this -> buckets.size();
            //当全局深度需要增加时，索引数量翻倍，但是因为是按照下标顺序排列，所以只需要按序把桶内的再塞进去一倍即可
            for(int i = 0; i < temp_size; i++)
            {
                this -> buckets.push_back(buckets[i]);
            }
            globalDepth++;
        }
        //索引数量翻倍，但是一次只会增加一个桶
        bucketsnum++;
        //使用与运算的取位性质，做一个取新的一位的取位变量
        int getNewBit = 1<<((current_bucket->localDepth) - 1);
        shared_ptr<Bucket>  new_bucket = make_shared<Bucket>(current_bucket->localDepth);
        typename map<K, V>::iterator p;
        //桶的分裂，根据新增加的深度位，把内容分到两个桶
        for(p = current_bucket->bucketMap.begin(); p != current_bucket->bucketMap.end();)
        {
            if(getNewBit & HashKey(p->first))
            {
                new_bucket->bucketMap[p->first] = p->second;
                p = current_bucket->bucketMap.erase(p);
            }else p++;
        }
        //遍历桶的数组，此时数组内存在两个指向当前桶的索引，把序号靠后的索引指向新的桶
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
