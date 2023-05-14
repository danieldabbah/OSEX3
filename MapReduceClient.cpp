#include <csignal>
#include <iostream>
#include <atomic>
#include <set>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include "MapReduceFramework.h"







class Job;

using namespace std;
struct ThreadContext{
    int threadID;
    Job* p_job;
    IntermediateVec* p_pesonalThreadVector;
};

class Job{
    private:
        const int multiThreadLevel;
        pthread_t* threads;
        ThreadContext* threadContexts;
        IntermediateVec* p_personalThreadVectors;
        const MapReduceClient& client;
        const InputVec& inputVec;
        OutputVec* outputVec;
        JobState state;
        atomic<uint64_t>* p_atomic_counter;
        // 0-30 counter, 31-61 input size, 62-63 stage
        std::set<int>* test;
        Barrier* p_afterSortBarrier;
        Barrier* p_afterShuffleBarrier;
        //TODO: add mutexes
        //TODO: in the destructor release the new
        pthread_mutex_t testMutex;
        vector<IntermediateVec*> intermediateVec;

    public:
        vector<IntermediateVec*> &getIntermediateVec(){
            return intermediateVec;
        }



        Job(const int multiThreadLevel,
            const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec):
                testMutex(PTHREAD_MUTEX_INITIALIZER),
                multiThreadLevel(multiThreadLevel), client(client), inputVec(inputVec){
            //TODO: check if new command fail
            outputVec = outputVec;
            threads = new pthread_t[multiThreadLevel];
            threadContexts = new ThreadContext[multiThreadLevel];
            p_personalThreadVectors = new IntermediateVec[multiThreadLevel];
            this->state = {UNDEFINED_STAGE,0};
            this->p_atomic_counter = new std::atomic<uint64_t>(inputVec.size() << 31);
            p_afterShuffleBarrier = new Barrier(multiThreadLevel);
            p_afterSortBarrier = new Barrier(multiThreadLevel);
        }

        int getMultiThreadLevel() const {
            return multiThreadLevel;
        }

        virtual ~Job() {
            free(this->threads);
            free(this->threadContexts);
            free(this->p_personalThreadVectors);
            free(this->p_atomic_counter);
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

        Barrier *getPAfterShuffleBarrier() const {
            return p_afterShuffleBarrier;
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

        pthread_mutex_t &getTestMutex() {
            return testMutex;
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

        IntermediateVec *getPpersonalVectors() const {
            return p_personalThreadVectors;
        }

        const InputVec &getInputVec() const {
            return inputVec;
        }

        const MapReduceClient &getClient() const {
            return client;
        }

        Barrier *getPAfterSortBarrier() const {
            return p_afterSortBarrier;
        }
};
void emit2 (K2* key, V2* value, void* context){
    auto* p_intermediateVec = (IntermediateVec*) context;
    p_intermediateVec->push_back(pair<K2*, V2*>(key, value));
}

bool cmpKeys(const IntermediatePair &a, const IntermediatePair &b){
    return *a.first < *b.first;
}
bool isEqualKeys(const IntermediatePair &a, const IntermediatePair &b){
    return !cmpKeys(a,b) &&!cmpKeys(b,a);
}

void mapAndSort(ThreadContext* tc){
    unsigned long int myIndex = 0;
    //the thread pick an index to work on:
    myIndex = tc->p_job->addAtomicCounter();
    while (myIndex  < tc->p_job->getAtomicCounterInputSize()){
        // if the index is ok, perform the appropriate function of the client of the pair in the index.
        // get the matching pair from the index:
        InputPair inputPair = tc->p_job->getInputVec().at(myIndex);

        tc->p_job->getClient().map(inputPair.first,inputPair.second,tc->p_pesonalThreadVector);
        myIndex = tc->p_job->addAtomicCounter();
    }
    // each thread will sort its intermidateVector:
    //sort(tc->p_pesonalThreadVector->begin(), tc->p_pesonalThreadVector->end(), cmpKeys);
}

void printVector2(vector<pair<K2*, V2*>> &vec, int vecId){
    cout<<endl;
    for (auto it = vec.begin(); it != vec.end(); it++){
        cout << "thread: " << vecId << " key:" ;
        it->first->print();
        cout << " value: ";
        it->second->print();
        cout << endl;
    }
}


/**
 * finds the index of the thread who has the largest key
 * @param tc thread context containing job
 * @return the index, -1 if the vectors are empty
 */
int findMaxKeyTid(ThreadContext* tc){
    K2 *maxKey = nullptr;
    int saveId = -1;
    vector<pair<K2*, V2*>>* curVector;
    for (int i = 0; i < tc->p_job->getMultiThreadLevel(); i++){
        curVector = &tc->p_job->getPpersonalVectors()[i];
        if (!curVector->empty()){
            if (maxKey == nullptr || *maxKey < *curVector->back().first) {
                maxKey = curVector->back().first;
                saveId = i;
            }
        }
    }
    return saveId;
}

void shuffle(ThreadContext* tc){ //TODO: advance the atomic counter after each phase counter and the phase itself. set the counter to 0 after each phase.
    unsigned long int outputSize = 0;
    for (int i = 0; i < tc->p_job->getMultiThreadLevel(); i++){
        outputSize += tc->p_job->getPpersonalVectors()[i].size();
    }
    unsigned long int count = 0;
    while(count <= outputSize){ // if count > outputsize, break
        int threadIdOfMax = findMaxKeyTid(tc); // find the index of the thread that contains the maximum key.
        IntermediateVec* p_currentVec = new IntermediateVec();
        IntermediatePair  currentPair = tc->p_job->getPpersonalVectors()[threadIdOfMax].back();
        // create intermidate vector

        for (int i = 0; i < tc->p_job->getMultiThreadLevel(); ++i) { // for every thread personal vector
            while(isEqualKeys(tc->p_job->getPpersonalVectors()[i].back(), currentPair)){ //get all equal key pairs
                p_currentVec->push_back(tc->p_job->getPpersonalVectors()[i].back());
                tc->p_job->getPpersonalVectors()[i].pop_back();
                count++;
            }

        }
        tc->p_job->getIntermediateVec().push_back(p_currentVec);
        // for loop to iterate over all the threads vctor
        // while loop to get all the keys each vector
    }
}

void* threadMainFunction(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    mapAndSort(tc);

    tc->p_job->getPAfterSortBarrier()->barrier();
    if (tc->threadID == 0) {
        shuffle(tc);
    }
    tc->p_job->getPAfterShuffleBarrier()->barrier();


    return nullptr;
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
        threadContexts[i] = {i,job, &(job->getPpersonalVectors()[i])};
    }

    //For loop that create all threads:
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&threads[i], NULL,threadMainFunction , &threadContexts[i]);
    }
    //TODO: remove pthread join (it need to be in another function), in the 'waitForJon' function
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }
    return static_cast<JobHandle> (job);
}




//    pthread_mutex_lock(tc->p_job->getMutexBeforeShuffle());
//    if (tc->p_job->isBeforeShuffle()){
//        if (tc->threadID == 0){
//            //perform shuffle
//            cout << "thread ID: " << tc->threadID << "  will preform a shuffle"<< endl;
//            tc->p_job->setBeforeShuffle(false);
//            pthread_cond_broadcast(tc->p_job->getCondBeforeShuffle());
//        } else {
//            cout << "thread ID: " << tc->threadID << "  gonna wait"<< endl;
//            pthread_cond_wait(tc->p_job->getCondBeforeShuffle(), tc->p_job->getMutexBeforeShuffle());
//            cout << "thread ID: " << tc->threadID << "  done waiting"<< endl;
//        }
//    }
//    pthread_mutex_unlock(tc->p_job->getMutexBeforeShuffle());
//    cout << "thread ID: " << tc->threadID << "  after mutex" << endl;


//
//    pthread_mutex_lock(&tc->p_job->getTestMutex());
//    printVector2(*tc->p_pesonalThreadVector, tc->threadID);
//    pthread_mutex_unlock(&tc->p_job->getTestMutex());