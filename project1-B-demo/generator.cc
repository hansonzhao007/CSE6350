#include <iostream>

#include <string.h>
#include <signal.h>
#include <cstdio>
#include <atomic>

#include <vector>
#include <string>
#include <pthread.h>
#include <sys/time.h>
#include <sstream>
#include <array>
#include <memory>

#include <algorithm>
#include <fstream>
#include <random>
#include <unistd.h>

auto rng = std::default_random_engine {};

class RandomKeyTrace {
public:
    RandomKeyTrace(size_t count) {
        count_ = count;
        keys_.resize(count);
        for (size_t i = 0; i < count; i++) {
            keys_[i] = i + 1;
        }
        Randomize();
    }

    ~RandomKeyTrace() {
    }

    void Randomize(void) {
        printf("Randomize the trace...\r");
        fflush(nullptr);
        std::shuffle(std::begin(keys_), std::end(keys_), rng);
    }
    
    class RangeIterator {
    public:
        RangeIterator(std::vector<size_t>* pkey_vec, size_t start, size_t end):
            pkey_vec_(pkey_vec),                        
            end_index_(end), 
            cur_index_(start) { }

        inline bool Valid() {
            return (cur_index_ < end_index_);
        }

        inline size_t Next() {
            return (*pkey_vec_)[cur_index_++];
        }

        std::vector<size_t>* pkey_vec_;
        size_t end_index_;
        size_t cur_index_;
    };

    class Iterator {
    public:
        Iterator(std::vector<size_t>* pkey_vec, size_t start_index, size_t range):
            pkey_vec_(pkey_vec),
            range_(range),                        
            end_index_(start_index % range_), 
            cur_index_(start_index % range_),
            begin_(true)  
        {
            
        }

        Iterator(){}

        inline bool Valid() {
            return (begin_ || cur_index_ != end_index_);
        }

        inline size_t Next() {
            begin_ = false;
            size_t index = cur_index_;
            cur_index_++;
            if (cur_index_ >= range_) {
                cur_index_ = 0;
            }
            return (*pkey_vec_)[index];
        }

        std::string Info() {
            char buffer[128];
            sprintf(buffer, "valid: %s, cur i: %lu, end_i: %lu, range: %lu", Valid() ? "true" : "false", cur_index_, end_index_, range_);
            return buffer;
        }

        std::vector<size_t>* pkey_vec_;
        size_t range_;
        size_t end_index_;
        size_t cur_index_;
        bool   begin_;
    };

    Iterator trace_at(size_t start_index, size_t range) {
        return Iterator(&keys_, start_index, range);
    }

    RangeIterator Begin(void) {
        return RangeIterator(&keys_, 0, keys_.size());
    }

    RangeIterator iterate_between(size_t start, size_t end) {
        return RangeIterator(&keys_, start, end);
    }

    size_t count_;
    std::vector<size_t> keys_;
};

void printInfo(char* binname) {
    printf("usage: %s -n 1000 -o input.txt -r 20\n", binname);
}

int main(int argc, char **argv) {
    
    int c;
    size_t nums = 100000;
    char buf[128] = "input.txt";
    char* trace_file = buf;
    int get_ratio = 100;   // in %. will generate nums * get_ratio% get requests.
    int put_ratio = 100;   // in %. will generate nums * put_ratio% put requests.
    int del_ratio = 0;     // in %. will generate nums * del_ratio% del requests.
    if (argc == 1) {
        printInfo(argv[0]);
        return 0;
    }
    while ((c = getopt(argc, argv, "n:o:g:p:d:")) != -1) {
        switch (c) {
        case 'n': nums = atoi(optarg); break;
        case 'o': trace_file = optarg; break;
        case 'g': get_ratio = atoi(optarg); if(get_ratio >= 100) get_ratio=100; break;
        case 'p': put_ratio = atoi(optarg); if(put_ratio >= 100) put_ratio=100; break;
        case 'd': del_ratio = atoi(optarg); if(del_ratio >= 100) del_ratio=100; break;
        case '?': printInfo(argv[0]); return 0;
        default:
        break;
        }
    }
    

    RandomKeyTrace trace(nums);
    std::ofstream outFile;
    outFile.open(trace_file);
    auto iter = trace.Begin();

    outFile << "type,key1,key2,value" << std::endl;
    size_t put_num = nums * put_ratio / 100;
    while (iter.Valid() && put_num--) {
        char buf[128];
        int key = iter.Next();
        sprintf(buf, "put,key%013d,,value%011d\n", key, key);
        outFile << buf;
    }

    auto iter2 = trace.Begin();
    size_t get_num = nums * get_ratio / 100;
    while (iter2.Valid() && get_num--) {
        char buf[128];
        int key = iter2.Next();
        sprintf(buf, "get,key%013d,,\n", key);
        outFile << buf;
    }

    auto iter3 = trace.Begin();
    size_t del_num = nums * del_ratio / 100;
    while (iter3.Valid() && del_num--) {
        char buf[128];
        int key = iter3.Next();
        sprintf(buf, "del,key%013d,,\n", key);
        outFile << buf;
    }

    auto iter4 = trace.Begin();
    size_t scan_num = 10;
    while (iter4.Valid() && scan_num--) {
        char buf[128];
        int key1 = iter4.Next();
        sprintf(buf, "scan,key%013d,key%013d,\n", key1, key1 + 5);
        outFile << buf;
    }

    return 0;
}