#include <string.h>
#include <signal.h>
#include <cstdio>
#include <atomic>
#include <iterator>
#include <vector>
#include <map>
#include <string>
#include<string_view>
#include <pthread.h>
#include <sys/time.h>
#include <sstream>
#include <iostream>
#include <array>
#include <memory>


#include <algorithm>
#include <fstream>
#include <random>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <error.h>
#include <cstdio>
#include <cstdlib>
#include <assert.h>

#include <boost/serialization/map.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

/**
 * @brief Class to parse csv format file.
 * https://stackoverflow.com/questions/1120140/how-can-i-read-and-parse-csv-files-in-c
 */
class CSVRow {
    public:
        std::string_view operator[](std::size_t index) const {
            return std::string_view(&m_line[m_data[index] + 1], m_data[index + 1] -  (m_data[index] + 1));
        }
        std::size_t size() const {
            return m_data.size() - 1;
        }
        void readNextRow(std::istream& str) {
            std::getline(str, m_line);

            m_data.clear();
            m_data.emplace_back(-1);
            std::string::size_type pos = 0;
            while((pos = m_line.find(',', pos)) != std::string::npos)
            {
                m_data.emplace_back(pos);
                ++pos;
            }
            // This checks for a trailing comma with no data after it.
            pos   = m_line.size();
            m_data.emplace_back(pos);
        }
    private:
        std::string         m_line;
        std::vector<int>    m_data;
};
std::istream& operator>>(std::istream& str, CSVRow& data) {
    data.readNextRow(str);
    return str;
}

class Index {
public:    
    Index(const std::string& path):
        path_(path) {
        DeserializedFromFile(path);
    }

    ~Index() {
        // save dram index to index file
        SerializeToFile(path_);
    }
    /**
     * @brief Insert the key and offset in the data log file
     *        into dram index
     * 
     * @param key 
     * @param offset 
     * @return true  : this is an update request
     * @return false : this is an new insertion
     */
    inline bool Put(const std::string& key, int off) {
        auto iter = index_.find(key);
        if (iter != index_.end()) {
            // there exists a same key
            iter->second = off;
            return true;
        } else {
            // new key
            index_[key] = off;
            return false;
        }
    }

    /**
     * @brief 
     * 
     * @param key 
     * @return std::pair<int, int> : first:  value size
     *                               second: value offset in the data log
     */
    inline int Get(const std::string& key) {
        auto iter = index_.find(key);
        if (iter != index_.end()) {
            return iter->second;
        } else {
            return -1;
        }
    }

    /**
     * @brief delete an key in the index 
     * 
     * @param key 
     * @return true  : the key has been successfully deleted
     * @return false : no such key exist in the index
     */
    inline bool Delete(const std::string& key) {
        auto iter = index_.find(key);
        if (iter != index_.end()) {
            index_.erase(key);
            return true;
        }
        return false;
    }

    /**
     * @brief Store index file to path
     * 
     * @param path 
     */
    void SerializeToFile(const std::string& path) {
        std::ofstream ss(path);
        boost::archive::text_oarchive oarch(ss);
        oarch << index_;
    }

    /**
     * @brief Recover index from path if index file exists
     * 
     * @param path 
     */
    void DeserializedFromFile(const std::string& path) {
        std::ifstream ss(path);
        if (!ss.good()) {
            // path does not exist
            return;
        }
        boost::archive::text_iarchive iarch(ss);
        iarch >> index_;
    }

private:
    std::unordered_map<std::string, int> index_; // key->offset
    std::string path_;
}; // end of class Index

class DataLog {
public:
    /**
     * @brief Construct a new Data Log object
     * 
     * @param path : the data log file path
     */
    DataLog(const std::string& path) {
        int flags = FileExists(path) ? (O_CREAT | O_APPEND) : (O_CREAT);
        flags |= O_RDWR;
        int fd = open(path.c_str(), flags, 0666);
        if (fd < 0) {
            printf("While open a file for appending %s, %d", path.c_str(), errno);
            exit(1);
        }
        fd_ = fd;
        struct stat buf;
        fstat(fd, &buf);
        offset_ = buf.st_size;
        printf("Open DataLog %s of size %u\n", path.c_str(), offset_);
    }

    ~DataLog() {
        close(fd_);
    }

    /**
     * @brief Append the data at the end of data log file, 
     *        return the offset where the data start
     * 
     * @param s 
     * @param size 
     * @return size_t 
     */
    size_t Append(const char* s, int size) {
        ssize_t write_result = ::pwrite(fd_, s, size, offset_);
        if (write_result == -1) {
            perror("Append error");
            exit(1);
        }
        size_t tmp = offset_;
        offset_ += size;
        return tmp;
    }

    /**
     * @brief Read `size` byte data from `offset` in data log file
     * 
     * @param buf : read buffer
     * @param size : read size
     * @param offset : offset in data log file
     */
    void Read(char* buf, int size, int offset) {
        ssize_t read_result = ::pread(fd_, buf, size, offset);
        if (read_result == -1) {
            perror("Read error");
            exit(1);
        }
    }

private:
    bool FileExists(const std::string& filename) {
        return ::access(filename.c_str(), F_OK) == 0;
    }

    bool CreateFile(const std::string& filename, size_t size) {
        int flag = O_RDWR | O_DIRECT | O_CREAT;
        int fd = open(filename.c_str(), flag  , 0666);
        if (fd < 0) {
            printf("Create %s fail. %s", filename.c_str(), strerror(-fd));
            return false;
        }
        int res = ftruncate(fd, size);
        if (res < 0) {
            printf("Set size %ld fail", size);
            return false;
        }
        fdatasync(fd);
        close(fd);
        return true;
    }

    int fd_;
    int offset_;
}; // end of class DataLog

enum KVType: unsigned char {
    kKVInsert = 1,
    kKVDelete = 2
};

struct KVItemMeta {
    KVType  type;
    int16_t key_len;
    int16_t val_len;
}__attribute__ ((__packed__));

static_assert(sizeof(KVItemMeta) == 5, "KVItemMeta should be 5 bytes");

/**
 * @brief Encode key value to an KV item in the log.
 *        | type | key_len | val_len |  key_data | val_data |
 *        |        meta              |         data         |
 * 
 * @param buf : encode buffer. make sure the buffer have enough space before encoding
 * @param key 
 * @param value 
 * @param type 
 * @return int : the length of the KV item after encoding 
 */
inline int EncodeKV(char* buf, const std::string& key, const std::string& val, KVType type) {
    // encode the meta part
    int16_t key_len = key.size();
    int16_t val_len = val.size();
    KVItemMeta meta;
    meta.type = type;
    meta.key_len = key_len;
    meta.val_len = val_len;
    memcpy(buf, &meta, sizeof(KVItemMeta));

    // encode the data part
    memcpy(buf + sizeof(KVItemMeta), key.data(), key_len);
    if (type == kKVInsert) {
        memcpy(buf + sizeof(KVItemMeta) + key_len, val.data(), val_len);
    } 
    return sizeof(KVItemMeta) + key_len + val_len;
}

inline bool DecodeKV( char* buf, std::string* key, std::string* val) {
    KVItemMeta* meta = reinterpret_cast<KVItemMeta*>(buf);
    key->assign(buf + sizeof(KVItemMeta),  meta->key_len);
    val->assign(buf + sizeof(KVItemMeta) + meta->key_len, meta->val_len);
    return true;
}

class KVStore {
public:
    KVStore(const std::string& path): 
        kv_index_(path + "kvstore.data.index"),
        kv_data_log_(path + "kvstore.data.datalog") {
    }

    /**
     * @brief 
     * 
     * @param key 
     * @param value 
     * @return true  : this is an update request
     * @return false : this is a new insertion
     */
    bool Put(const std::string& key, const std::string& value) {
        static char buf[1024];
        // step 0. Construct KV item
        int item_len = EncodeKV(buf, key, value, kKVInsert);

        // step 1. Append the kv item to data log
        size_t off = kv_data_log_.Append(buf, item_len);

        // step 2. Add the key->off to kv_index_
        bool res = kv_index_.Put(key, off);
        return res;
    }

    /**
     * @brief Obtain the value of key if key exists
     * 
     * @param key 
     * @param value 
     * @return true  : successfully obtain the value
     * @return false : no such key
     */
    bool Get(const std::string& key, std::string* value) {
        static char buf[1024];
        // step 1. Check if the key exists
        int off = kv_index_.Get(key);
        if (off < 0) {
            // no key exists
            return false;
        }

        // step 2. Read the meta part
        kv_data_log_.Read(buf, sizeof(KVItemMeta), off);
        KVItemMeta* meta = reinterpret_cast<KVItemMeta*>(buf);
        KVType type = meta->type;
        int key_len = meta->key_len;
        int val_len = meta->val_len;
        if (type == kKVDelete) {
            // this is an deleted item
            return false;
        }
        
        // step 3. Read the data part
        kv_data_log_.Read(buf, val_len, off + sizeof(KVItemMeta) + key_len);
        value->assign(buf, val_len);
        return true;
    }

    /**
     * @brief Delete the key from index
     * 
     * @param key 
     * @return true  : the existing key has been deleted
     * @return false : no such key exists
     */
    bool Delete(const std::string& key) {
        static char buf[1024];
        // step 0. Construct KV item
        int item_len = EncodeKV(buf, key, "", kKVDelete);

        // step 1. Append the kv item to data log
        size_t off = kv_data_log_.Append(buf, item_len);

        // step 2. Add the key->off to kv_index_
        bool res = kv_index_.Put(key, off);
        return res;
    }

private:
    Index   kv_index_;
    DataLog kv_data_log_;
};

void printInfo(char* binname) {
    printf("usage: %s -i input.txt -o output.txt\n", binname);
}

int main(int argc, char **argv) {
    // parse the parameter
    int c;
    char buf1[128] = "input.txt";
    char buf2[128] = "output.txt";
    char* input_file  = buf1;
    char* output_file = buf2;
    if (argc == 1) {
        printInfo(argv[0]);
        return 0;
    }
    while ((c = getopt(argc, argv, "i:o:")) != -1) {
        switch (c) {        
            case 'i': input_file  = optarg; break;
            case 'o': output_file = optarg; break;
            case '?': printInfo(argv[0]); return 0;
            default:
            break;
        }
    }

    // Create a kv store in current folder or open if kvstore already exists
    KVStore kvstore("./");

    // open the input file and output file
    std::ifstream file(input_file);
    std::ofstream outfile(output_file);
    outfile << "type,key1,outcome,values\n";
    CSVRow row;
    while(file >> row) {
        std::string type(row[0]);
        std::string key1(row[1]);
        std::string key2(row[2]);
        std::string value(row[3]);
        if (type == "put") {
            bool res = kvstore.Put(key1, value);
            outfile << type << "," << key1 << "," << (res ? 1 : 0) << ",\n";
        } else if (type == "get") {
            bool res = kvstore.Get(key1, &value);            
            outfile << type << "," << key1 << "," << (res ? 1 : 0) << "," << value << "\n";            
        } else if (type == "del") {
            bool res = kvstore.Delete(key1);
            outfile << type << "," << key1 << "," << (res ? 1 : 0) << ",\n";
        } else if (type == "scan") {
            // TODO
        } else {

        }
    }

    
    return 0;
}