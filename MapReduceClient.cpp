#include <csignal>
#include <iostream>
#include <atomic>
#include <set>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

class Job;

using namespace std;
struct ThreadContext{
    int threadID;
    Job* p_job;
    IntermediateVec* p_intermediateVector;
};
//safafas
class Job{
    private:
        const int multiThreadLevel;
        pthread_t* threads;
        ThreadContext* threadContexts;
        IntermediateVec* p_intermediateVectors;
        const MapReduceClient& client;
        const InputVec& inputVec;
        OutputVec* outputVec;
        JobState state;
        std::atomic<uint64_t>* p_atomic_counter; // 0-30 counter, 31-61 input size, 62-63 stage
        std::set<int>* test;
        //TODO: add mutexes
        //TODO: in the destructor relase the new


    public:
    pthread_mutex_t * counter_mutex;

        Job(const int multiThreadLevel,
            const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec):
                multiThreadLevel(multiThreadLevel), client(client), inputVec(inputVec){
            //TODO: check if new command fail
            //TODO: add atomic counter<int 64 bit>
            outputVec = outputVec;
            threads = new pthread_t[multiThreadLevel];
            threadContexts = new ThreadContext[multiThreadLevel];
            p_intermediateVectors = new IntermediateVec[multiThreadLevel];
            this->state = {UNDEFINED_STAGE,0};
            this->p_atomic_counter = new std::atomic<uint64_t>(inputVec.size() << 31);
            this->test = new std::set<int>();
            this->counter_mutex = new pthread_mutex_t();
            pthread_mutex_init(counter_mutex,NULL);
        }

        unsigned long int addAtomicCounter(){

           return ((*this->p_atomic_counter)++) & (0x7fffffff);
        }

        void setAtomicCounterInputSize(int size){
            (*this->p_atomic_counter) = ((*this->p_atomic_counter)&(~0x3fffffff80000000))|((unsigned long int)size << 31);
        }

        void addStageAtomicCounter(){
            (*this->p_atomic_counter) += ((unsigned long int)1 << 62);
        }

        unsigned long int getAtomicCounterCurrent(){
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
        OutputVec* getOutputVector(){
            return this->outputVec;
        }
        std::set<int> *getTest(){
            return this->test;
        }

    IntermediateVec *getPIntermediateVectors() const {
        return p_intermediateVectors;
    }
};




void* threadMainFunction(void* arg)
{
    ThreadContext* threadContext = (ThreadContext*) arg;

    unsigned long int myIndex = 0;
    for (int i = 0; i < 1000000; i++) {

        myIndex = threadContext->p_job->addAtomicCounter();
        std::cout << "Hello, Im thread number: " << threadContext->threadID <<"\nMy index is: "<<myIndex<<'\n';
        pthread_mutex_lock(threadContext->p_job->counter_mutex);
        threadContext->p_job->getTest()->insert(myIndex);
        threadContext->p_job->getTest()->insert(myIndex);
        pthread_mutex_unlock(threadContext->p_job->counter_mutex);

    }
    return 0;
}


void error_handler_function(const std::string& inputMessage){
    std::cerr<<"system error: "<<inputMessage<<std::endl;
    //TODO: free all allocate resorces
    exit(EXIT_FAILURE);
}

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

    //For loop that init all thread contexts:
    for (int i = 0; i < multiThreadLevel; ++i) {
        threadContexts[i] = {i,job};
    }

    //For loop that create all threads:
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&threads[i], NULL,threadMainFunction , &threadContexts[i]);
    }
    //TODO: remove pthread join (it need to be in another function), in the 'waitForJon' function
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }
    std::cout<<std::endl<< "size of test is: "<<job->getTest()->size();
    return static_cast<JobHandle> (job);
}

void emit2 (K2* key, V2* value, void* context){
    auto* p_intermediateVec = (IntermediateVec*) context;
    p_intermediateVec->push_back(pair<K2*, V2*>(key, value));
}