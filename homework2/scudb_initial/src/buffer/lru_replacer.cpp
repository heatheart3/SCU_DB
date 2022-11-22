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
//插入功能：如果是已经存在的页面，那么相当于近期再次调用，需要提前；如果是新插入的页面，那么直接插到前面
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        lock_guard<mutex> lck(latch);
        shared_ptr<Node> temp_cur;
        //先查询是否该页面已存在
        if(pagemap.find(value) != pagemap.end())
        {
            //存在就把这个页面取出来
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
            //不存在说明是新进来的，新建一个
            temp_cur = make_shared<Node>(value);
        }
        //插到头部，保证按照最近使用时间进行排序
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
//取出最不常用页
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        lock_guard<mutex> lck(latch);
        //非空的时候才能往外取
        if(pagemap.size() != 0)
        {
            //因为按照时间排序，所以取出最后一个即可
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
//删除指定页面
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

