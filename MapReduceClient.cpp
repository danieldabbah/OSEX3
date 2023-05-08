#include <csignal>
#include <iostream>
#include <atomic>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
struct ThreadContext{
    int threadID;
    int startIndex;
    int endIndex;

};

class Job{
public:
    Job(const int multiThreadLevel,
        const MapReduceClient& client,
        const InputVec& inputVec, OutputVec& outputVec):
            multiThreadLevel(multiThreadLevel),
            client(client),
            inputVec(inputVec)


            {
        //TODO: check if new command fail
        outputVec = outputVec;

        threads = new pthread_t[multiThreadLevel];
        threadContexts = new ThreadContext[multiThreadLevel];
        this->state = {UNDEFINED_STAGE,0};
        this->p_atomic_counter = new std::atomic<uint64_t>(inputVec.size() << 31);
    }
private:
    const int multiThreadLevel;
    pthread_t* threads;
    ThreadContext* threadContexts;
    const MapReduceClient& client;
    const InputVec inputVec;
    OutputVec* outputVec;
    JobState state;
    std::atomic<uint64_t>* p_atomic_counter; // 0-30 counter, 31-61 input size, 62-63 stage
    //TODO: add mutexes

//TODO: in the destructor relase the new
public:
    void addAtomicCounter(){
        (*this->p_atomic_counter) += 1;
    }

    void setAtomicCounterInputSize(int size){
        (*this->p_atomic_counter) = ((*this->p_atomic_counter)&(~0x3fffffff80000000))|((unsigned long int)size << 31);
    }

    void addStageAtomicCounter(){
        (*this->p_atomic_counter) += ((unsigned long int)1 << 62);
    }

    unsigned long int getAtomicCounterp_job->addStageAtomicCounter();Current(){
        return (this->p_atomic_counter->load()) & (0x7fffffff);
    }

    unsigned long int getAtomicCounterInputSize(){
        return ((this->p_atomic_counter->load()) >> 31) & (0x7fffffff);
    }

    stage_t getAtomicCounterState(){
        return stage_t((this->p_atomic_counter->load()) >> 62);
    }

    const std::atomic<uint64_t>* getAtomicCounter(){
        return this->p_atomic_counter;
    }
};





// TODO: create a thread context class/ struct that have thread id, and relavent data
//TODO: create a job class
// TODO: create the "main" function for each thread
//TODO: add to the job class all the init of the input vector, interminate vector, output vector
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    // allocate a job class on the heap
    Job* job = new Job(multiThreadLevel,client,inputVec,outputVec);
    // create all the threads:
    pthread_t threads[multiThreadLevel];
    ThreadContext threadContexts[multiThreadLevel];
    // set the job class to have access to the thread pointer


    return static_cast<JobHandle> (job);
}



























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
#include <bitset>

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

void print_binary(unsigned long int num){
    std::bitset<sizeof(unsigned long int)*8> bits(num);
    std::cout << bits.to_string() << std::endl;
}

int main(int argc, char** argv)
{
    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome");
    VString s3("race conditions are bad");
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});
    inputVec.push_back({nullptr, &s3});
    inputVec.push_back({nullptr, &s3});
    JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
    Job *p_job = static_cast<Job*>(job);
    print_binary((p_job->getAtomicCounter()->load()));
    for (unsigned long int i=0; i < 31; i++){
        p_job->addAtomicCounter();
    }
    print_binary((p_job->getAtomicCounter()->load()));
    p_job->addStageAtomicCounter();
    p_job->addStageAtomicCounter();
    std::cout<<p_job->getAtomicCounterCurrent()<<std::endl;
    std::cout<<p_job->getAtomicCounterInputSize()<<std::endl;
    std::cout<<p_job->getAtomicCounterState()<<std::endl;

    return 0;
}
