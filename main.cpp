#include <pthread.h>
#include <cstdio>
#include <atomic>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "MapReduceFramework.h"
#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>


#define MT_LEVEL 4

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};

class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};

class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;
};


class CounterClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const {
        std::array<int, 256> counts;
        counts.fill(0);
        for(const char& c : static_cast<const VString*>(value)->content) {
            counts[(unsigned char) c]++;
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar* k2 = new KChar(i);
            VCount* v2 = new VCount(counts[i]);
            usleep(150000);
            //emit2(k2, v2, context);
        }
    }

    virtual void reduce(const IntermediateVec* pairs,
                        void* context) const {
        const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
        int count = 0;
        for(const IntermediatePair& pair: *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        KChar* k3 = new KChar(c);
        VCount* v3 = new VCount(count);
        usleep(150000);
        //emit3(k3, v3, context);
    }
};


struct ThreadContext {
    int threadID;
    std::atomic<uint32_t>* atomic_counter;
};


void* count(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;

    int n = 1000;
    for (int i = 0; i < n; ++i) {
        if (i % 10 == 0){
            (*(tc->atomic_counter))++;
        }
        if (i % 100 == 0){
            (*(tc->atomic_counter)) += 1 << 16;
        }
    }
    (*(tc->atomic_counter)) += tc->threadID % 2 << 30;

    return 0;
}


int main(int argc, char** argv)
{
    pthread_t threads[MT_LEVEL];
    ThreadContext contexts[MT_LEVEL];
    std::atomic<uint32_t> atomic_counter(0);

    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {i, &atomic_counter};
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, count, contexts + i);
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_join(threads[i], NULL);
    }
    // Note that 0b is in the standard only from c++14
    /* printf("atomic counter first 16 bit: %d\n", atomic_counter.load() & (0b1111111111111111)); */


    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome");
    VString s3("race conditions are bad");
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});

    startMapReduceJob(client, inputVec, outputVec, 4);





    return 0;
}
