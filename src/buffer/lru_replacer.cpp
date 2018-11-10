/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template<typename T>
    LRUReplacer<T>::LRUReplacer() {}

    template<typename T>
    LRUReplacer<T>::~LRUReplacer() {}

    /*
     * Insert value into LRU
     */
    template<typename T>
    void LRUReplacer<T>::Insert(const T &value) {
        mtx.lock();
        size_t i = 0;
        for (; i < queue.size(); ++i) {
            if (queue[i] == value) {
                break;
            }
        }
        if (i == queue.size()) {
            queue.push_back(value);
        } else {
            T tempValue = queue[i];
            queue.erase(queue.begin() + i);
            queue.push_back(tempValue);
        }
        mtx.unlock();
    }

    /* If LRU is non-empty, pop the head member from LRU to argument "value", and
     * return true. If LRU is empty, return false
     */
    template<typename T>
    bool LRUReplacer<T>::Victim(T &value) {
        mtx.lock();
        if (queue.empty()) {
            mtx.unlock();
            return false;
        } else {
            value = queue.front();
            queue.pop_front();
            mtx.unlock();
            return true;
        }
    }

    /*
     * Remove value from LRU. If removal is successful, return true, otherwise
     * return false
     */
    template<typename T>
    bool LRUReplacer<T>::Erase(const T &value) {
        mtx.lock();
        size_t i = 0;
        bool find = false;
        while (i < queue.size()) {
            if (queue[i] == value) {
                queue.erase(queue.begin() + i);
                find = true;
            } else {
                ++i;
            }
        }
        mtx.unlock();
        return find;
    }

    template<typename T>
    size_t LRUReplacer<T>::Size() {
        return queue.size();
    }

    template
    class LRUReplacer<Page *>;

    // test only
    template
    class LRUReplacer<int>;

} // namespace cmudb
