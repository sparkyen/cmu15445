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
    if(Size()==0) return false;
    std::scoped_lock lock{mutex};
    size_t now = hand;
    do{
        printf("++++++++ now1 = %lu  ++++++++\n", now);
        if(now==replacer.size()) now = 0;
        if(!replacer[now].contained) {
            now++;
            continue;
        }
        printf("++++++++ now2 = %lu  ++++++++\n", now);
        if(replacer[now].ref){
            replacer[now].ref = 0;
            now++;
        }
        else {
            replacer[now].contained = 0;
            *frame_id = now, hand = now+1;
            printf("++++++++ *frame_id = %d  ++++++++\n", (*frame_id));
            return true;
        }
    }while(now!=hand);
    return false; 
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock{mutex};
    replacer[frame_id].contained = 0;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lock{mutex};
    if(!replacer[frame_id].contained){
        replacer[frame_id].contained = 1;
        replacer[frame_id].ref = 1;
        printf("######### frame_id = %d  #########\n", frame_id);   
    }
}

size_t ClockReplacer::Size() { 
    std::scoped_lock lock{mutex};
    size_t tot = 0;
    for(size_t i = 0; i < replacer.size(); i++)
        if(replacer[i].contained) tot++;
    return tot;
}

}  // namespace bustub
