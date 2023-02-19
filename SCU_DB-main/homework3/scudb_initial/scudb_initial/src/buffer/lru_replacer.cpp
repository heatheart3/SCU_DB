/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {
        this -> head = make_shared<Node>();
        this -> tail = make_shared<Node>();
        head -> next = tail;
        tail -> prev = head;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        lock_guard<mutex> lck(latch);
        shared_ptr<Node> temp_cur;
        if(pagemap.find(value) != pagemap.end())
        {
            temp_cur = pagemap[value];
            shared_ptr<Node> temp_prev;
            shared_ptr<Node> temp_next;
            temp_prev = temp_cur -> prev;
            temp_next = temp_cur -> next;
            temp_next -> prev = temp_prev;
            temp_prev -> next = temp_next;

        }
        else
        {
            temp_cur = make_shared<Node>(value);
        }

        shared_ptr<Node> temp_node;
        temp_node = head -> next;
        head -> next = temp_cur;
        temp_cur -> prev = head;
        temp_cur -> next = temp_node;
        temp_node-> prev = temp_cur;
        pagemap[value] = temp_cur;
        return ;
    }

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        lock_guard<mutex> lck(latch);
        if(pagemap.size() != 0)
        {
            shared_ptr<Node> temp_last_node = tail -> prev;
            shared_ptr<Node> temp_post_node = (tail -> prev) -> prev;
            temp_post_node -> next = tail;
            tail -> prev = temp_post_node;
            value = temp_last_node->val;
            pagemap.erase(value);
            return true;
        }
        return false;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
    template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        lock_guard<mutex> lck(latch);
        if(!pagemap.empty() && pagemap.find(value) != pagemap.end())
        {
            shared_ptr<Node> temp_node = pagemap[value];
            temp_node->prev->next = temp_node->next;
            temp_node->next->prev = temp_node->prev;

        }
        return pagemap.erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(latch);
        return pagemap.size(); }

    template class LRUReplacer<Page *>;
// test only
    template class LRUReplacer<int>;

} // namespace scudb

