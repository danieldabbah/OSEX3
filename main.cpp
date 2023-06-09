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
#include <iostream>


#define MT_LEVEL 4

using namespace std;

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    virtual void print(){std::cout << content;}
    std::string content;
};

class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual void print(){std::cout<<c;}
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};

class VCount : public V2, public V3{
public:
    virtual void print() {std::cout << count;}
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
            emit2(k2, v2, context);
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
        emit3(k3, v3, context);
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
    for (int j = 0; j < 200; j++) {
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
        OutputVec *outputVec = new OutputVec();
        VString *s1 = nullptr;
        VString *s2 = nullptr;
        VString *s3 = nullptr;
        for (int i = 0; i < 1; i++) {
            s1 = new VString("ab");
            s2 = new VString("da");
            s3 = new VString("aeeeeeeeeee");
            inputVec.push_back({nullptr, s1});
            inputVec.push_back({nullptr, s2});
            inputVec.push_back({nullptr, s3});
        }
//    for (int i = 0; i < 100; ++i) {
//        inputVec.push_back({nullptr, &s3});
//    }
        JobHandle job = startMapReduceJob(client, inputVec, *outputVec, 4);
        JobState state;
        JobState last_state={UNDEFINED_STAGE,0};
        getJobState(job, &state);
        while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
        {
            if (last_state.stage != state.stage || last_state.percentage != state.percentage){
                printf("stage %d, %f%% \n",
                       state.stage, state.percentage);
            }
            usleep(1000);
            last_state = state;
            getJobState(job, &state);
        }
        waitForJob(job);
        std::cout << "done!" << std::endl;

    }


    return 0;
}