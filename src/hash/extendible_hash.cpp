#include <functional>
#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

    /*
     * constructor
     * array_size: fixed array size for each bucket
     */
    template<typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size): bucketSize(size) {
        auto *bucketPtr = new Bucket(size, 0);
        bucketList = {bucketPtr};
        globalDepth = 0;
    }

    /*
     * helper function to calculate the hashing address of input key
     */
    template<typename K, typename V>
    size_t ExtendibleHash<K, V>::HashKey(const K &key) {
        std::hash<K> hashFunc;
        size_t hk = 0, h = hashFunc(key);
        for (size_t i = 0; i < bitNum; ++i) {
            hk |= ((h >> i) & 1) << (bitNum - 1 - i);
        }
        return (hk >> (bitNum - globalDepth)) & ((1 << globalDepth) - 1);
    }

    /*
     * helper function to return global depth of hash table
     * NOTE: you must implement this function in order to pass test
     */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetGlobalDepth() const {
        return globalDepth;
    }

    /*
     * helper function to return local depth of one specific bucket
     * NOTE: you must implement this function in order to pass test
     */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
        return bucketList[bucket_id]->localDepth;
    }

    /*
     * helper function to return current number of bucket in hash table
     */
    template<typename K, typename V>
    int ExtendibleHash<K, V>::GetNumBuckets() const {
        return static_cast<int>(bucketList.size());
    }

    /*
     * lookup function to find value associate with input key
     */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
        mtx.lock();
        size_t h = HashKey(key);
        for (auto kv : bucketList[h]->kvs) {
            if (key == kv.first) {
                value = kv.second;
                mtx.unlock();
                return true;
            }
        }
        mtx.unlock();
        return false;
    }

    /*
     * delete <key,value> entry in hash table
     * Shrink & Combination is not required for this project
     */
    template<typename K, typename V>
    bool ExtendibleHash<K, V>::Remove(const K &key) {
        mtx.lock();
        size_t hk = HashKey(key);
        for (size_t i = 0; i < bucketList[hk]->kvs.size(); ++i) {
            if (bucketList[hk]->kvs[i].first == key) {
                bucketList[hk]->kvs.erase(bucketList[hk]->kvs.begin() + i);
                ++bucketList[hk]->freeCnt;
                mtx.unlock();
                return true;
            }
        }
        mtx.unlock();
        return false;
    }

    /*
     * insert <key,value> entry in hash table
     * Split & Redistribute bucket when there is overflow and if necessary increase
     * global depth
     */
    template<typename K, typename V>
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        V val;
        if (Find(key, val)) {
            size_t hk = HashKey(key);
            mtx.lock();
            for (auto &kv : bucketList[hk]->kvs) {
                if (kv.first == key) {
                    kv.second = value;
                    break;
                }
            }
            mtx.unlock();
            return;
        }

        mtx.lock();
        while (true) {
            size_t hk = HashKey(key);
            Bucket *bucket = bucketList[hk];
            if (bucket->freeCnt > 0) {
                InsertIntoBucket(bucket, key, value);
                break;
            } else if (bucket->localDepth == globalDepth) {
                size_t bucketNum = bucketList.size();
                std::vector<Bucket *> tempBucketList(bucketList);
                for (size_t i = 0; i < bucketNum; ++i) {
                    bucketList.push_back(nullptr);
                }
                for (size_t i = 0; i < bucketNum << 1; ++i) {
                    bucketList[i] = tempBucketList[i >> 1];
                }
                ++globalDepth;
                bucketList[hk << 1] = new Bucket(bucketSize, globalDepth);
                bucketList[(hk << 1) + 1] = new Bucket(bucketSize, globalDepth);
                for (auto &kv : bucket->kvs) {
                    size_t hk = HashKey(kv.first);
                    InsertIntoBucket(bucketList[hk], kv.first, kv.second);
                }
                delete bucket;
            } else {
                int deltaDepth = globalDepth - bucket->localDepth;
                size_t stIndex = (hk >> deltaDepth) << deltaDepth;
                size_t edIndex = ((hk >> deltaDepth) + 1) << deltaDepth;
                size_t midIndex = stIndex + (edIndex - stIndex) / 2;
                auto newBucket1 = new Bucket(bucketSize, bucket->localDepth + 1);
                auto newBucket2 = new Bucket(bucketSize, bucket->localDepth + 1);
                for (size_t i = stIndex; i < edIndex; ++i) {
                    bucketList[i] = (i < midIndex) ? newBucket1 : newBucket2;
                }
                for (auto &kv : bucket->kvs) {
                    size_t hk = HashKey(kv.first);
                    InsertIntoBucket(bucketList[hk], kv.first, kv.second);
                }
                delete bucket;
            }
        }
        mtx.unlock();
    }

    template<typename K, typename V>
    void ExtendibleHash<K, V>::InsertIntoBucket(Bucket *const bucket, const K &key, const V &value) {
        --bucket->freeCnt;
        bucket->kvs.emplace_back(key, value);
    }

    template
    class ExtendibleHash<page_id_t, Page *>;

    template
    class ExtendibleHash<Page *, std::list<Page *>::iterator>;

    // test purpose
    template
    class ExtendibleHash<int, std::string>;

    template
    class ExtendibleHash<int, std::list<int>::iterator>;

    template
    class ExtendibleHash<int, int>;
} // namespace cmudb
