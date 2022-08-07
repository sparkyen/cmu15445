//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// replacer.cpp
//
// Identification: src/buffer/replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    ClockItem item = {0, 0};
    for(size_t i = 0; i < num_pages; i++){
        replacer.push_back(item);
    }
    hand = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    //@先执行Size()否则Victim会一直占用锁而造成阻塞
    size_t len = Size();
    std::scoped_lock lock{mutex};
    while(len>0){
        //printf("========== hand = %lu ==========\n", hand);
        if(replacer[hand].contained) {
            if(replacer[hand].ref)
                replacer[hand].ref = 0;
            else {
                //@按照算法这里不能将hand++
                replacer[hand].contained = 0;
                *frame_id = hand;
                //printf("\n");
                return true;
            }
        }
        hand = (hand+1)%replacer.size();
    }
    return false; 
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock{mutex};
    replacer[frame_id].contained = 0;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lock{mutex};
    /*
    @不能加上if(!replacer[frame_id].contained)判断
    因为可能会Unpin一个frame多次，而其标志位可能在Vitim中就被修改了
    */
    replacer[frame_id].contained = 1;
    replacer[frame_id].ref = 1;
    //printf("######### frame_id = %d  #########\n", frame_id);   
}

size_t ClockReplacer::Size() { 
    std::scoped_lock lock{mutex};
    size_t tot = 0;
    for(size_t i = 0; i < replacer.size(); i++)
        if(replacer[i].contained) tot++;
    //printf("######### tot = %lu  #########\n", tot);
    return tot;
}

}  // namespace bustub
