#include <csignal>
#include <iostream>
#include <atomic>
#include <set>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include "MapReduceFramework.h"





//TODO: for each data type, check if we need to add mutex to it.



class Job;
void* threadMainFunction(void* arg);

using namespace std;
struct ThreadContext{
    int threadID;
    Job* p_job;
    IntermediateVec* p_personalThreadVector;
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
        atomic<uint64_t> atomic_counter;        // 0-30 counter, 31-61 input size, 62-63 stage
        Barrier* p_afterSortBarrier;
        Barrier* p_afterShuffleBarrier;
        //TODO: in the destructor release the new
        pthread_mutex_t outputVectorMutex;
        pthread_mutex_t atomicCounterMutex;
        vector<IntermediateVec*> intermediateVec;
        bool waitCalled = false;

    public:
        Job(const int multiThreadLevel,
            const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec):
                atomic_counter(inputVec.size() << 31), atomicCounterMutex(PTHREAD_MUTEX_INITIALIZER),
                outputVectorMutex(PTHREAD_MUTEX_INITIALIZER), intermediateVec(),
                multiThreadLevel(multiThreadLevel), client(client), inputVec(inputVec){
            this->outputVec = &outputVec;
            this->threads = new pthread_t[multiThreadLevel];
            this->threadContexts = new ThreadContext[multiThreadLevel];
            this->p_personalThreadVectors = new IntermediateVec[multiThreadLevel];
            this->p_afterShuffleBarrier = new Barrier(multiThreadLevel);
            this->p_afterSortBarrier = new Barrier(multiThreadLevel);
            for (int i = 0; i < multiThreadLevel; ++i) {
                threadContexts[i] = {i, this, &(p_personalThreadVectors[i])};
            }
            for (int i = 0; i < multiThreadLevel; ++i) {
                pthread_create(&threads[i], NULL,threadMainFunction , &threadContexts[i]);
            }
        }



        virtual ~Job() {
            while (!this->intermediateVec.empty()){
                free(this->intermediateVec.back());
                this->intermediateVec.pop_back();
            }
            free(this->threads);
            free(this->threadContexts);
            free(this->p_personalThreadVectors);
            free(this->p_afterShuffleBarrier);
            free(this->p_afterSortBarrier);
            if(pthread_mutex_destroy(&this->outputVectorMutex) != 0){
                //TODO: handle error
            }
        }

    //========================= get and set ======================================
        int getMultiThreadLevel() const {
            return multiThreadLevel;
        }

        vector<IntermediateVec*> &getIntermediateVec(){
            return intermediateVec;
        }

        Barrier *getPAfterShuffleBarrier() const {
            return p_afterShuffleBarrier;
        }

        pthread_mutex_t &getOutputVectorMutex() {
            return outputVectorMutex;
        }

        OutputVec* getOutputVector(){
            return this->outputVec;
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

        bool wasWaitCalled(){
            return waitCalled;
        }

        void setWaitCalled(bool waitCalled) {
            Job::waitCalled = waitCalled;
        }

        pthread_t *getThreads() const {
            return threads;
        }

        pthread_mutex_t &getAtomicCounterMutex() {
            return atomicCounterMutex;
        }

    //========================= atomic counter ======================================

        unsigned long int addAtomicCounter(){
            pthread_mutex_lock(&atomicCounterMutex);
            unsigned long int ret =  (0x7fffffff) & this->atomic_counter++;
            pthread_mutex_unlock(&atomicCounterMutex);
            return ret;
        }

        void setAtomicCounterInputSize(unsigned long int size){
            pthread_mutex_lock(&atomicCounterMutex);
            (this->atomic_counter) = ((this->atomic_counter) & (~0x3fffffff80000000)) | ((unsigned long int)size << 31);
            pthread_mutex_unlock(&atomicCounterMutex);
        }

        void addStageAtomicCounter(){
            pthread_mutex_lock(&atomicCounterMutex);
            (this->atomic_counter) += ((unsigned long int)1 << 62);
            pthread_mutex_unlock(&atomicCounterMutex);
        }

        void resetAtomicCounterCount(){
            pthread_mutex_lock(&atomicCounterMutex);
            this->atomic_counter = (this->atomic_counter) & (0xffffffff80000000);
            pthread_mutex_unlock(&atomicCounterMutex);
        }

        unsigned long int getAtomicCounterCurrent(){
            pthread_mutex_lock(&atomicCounterMutex);
            unsigned long int ret = (this->atomic_counter.load()) & (0x7fffffff);
            pthread_mutex_unlock(&atomicCounterMutex);
            return ret;
        }

        unsigned long int getAtomicCounterInputSize(){
            pthread_mutex_lock(&atomicCounterMutex);
            unsigned long int ret = ((this->atomic_counter.load()) >> 31) & (0x7fffffff);
            pthread_mutex_unlock(&atomicCounterMutex);
            return ret;
        }

        stage_t getAtomicCounterState(){
            pthread_mutex_lock(&atomicCounterMutex);
            stage_t ret = stage_t((this->atomic_counter.load()) >> 62);
            pthread_mutex_unlock(&atomicCounterMutex);
            return ret;
        }

        void setAtomicCounterStage(unsigned long int stage){
            pthread_mutex_lock(&atomicCounterMutex);
            this->atomic_counter = ((this->atomic_counter) & (0x3fffffffffffffff)) | (stage << 62);
            pthread_mutex_unlock(&atomicCounterMutex);
        }
};


//========================= helpers to delete ======================================
void printVector2(vector<pair<K2*, V2*>> &vec, int threadId){
    cout<<endl;
    for (auto it = vec.begin(); it != vec.end(); it++){
        cout << "thread: " << threadId << " key:" ;
        it->first->print();
        cout << " value: ";
        it->second->print();
        cout << endl;
    }
}

void printVector3(vector<pair<K3*, V3*>> &vec, int vecId){
    cout<<endl;
    for (auto it = vec.begin(); it != vec.end(); it++){
        cout << "thread: " << vecId << " key:" ;
        it->first->print();
        cout << " value: ";
        it->second->print();
        cout << endl;
    }
}

//========================= map and sort phase ======================================


void emit2 (K2* key, V2* value, void* context){
    auto p_intermediateVec = (IntermediateVec*) context;
    p_intermediateVec->push_back(pair<K2*, V2*>(key, value));
}

bool cmpKeys(const IntermediatePair &a, const IntermediatePair &b){
    return *a.first < *b.first;
}

void mapAndSort(ThreadContext* tc){
    unsigned long int myIndex = 0;
    tc->p_job->setAtomicCounterStage(1);
    myIndex = tc->p_job->addAtomicCounter();     //the thread pick an index to work on:
    cout << " " << myIndex << " " << "my id = " <<tc->threadID <<  endl ;
    while (myIndex  < tc->p_job->getAtomicCounterInputSize()){

        // if the index is ok, perform the appropriate function of the client of the pair in the index.
        // get the matching pair from the index:
        InputPair inputPair = tc->p_job->getInputVec().at(myIndex);
        tc->p_job->getClient().map(inputPair.first,inputPair.second,tc->p_personalThreadVector);
        myIndex = tc->p_job->addAtomicCounter();     //the thread pick an index to work on:
        cout << " " << myIndex << " " << "my id = " <<tc->threadID << endl ;}

    sort(tc->p_personalThreadVector->begin(), tc->p_personalThreadVector->end(), cmpKeys);
}

//========================= shuffle phase ======================================


bool isEqualKeys(const IntermediatePair &a, const IntermediatePair &b){
    return !cmpKeys(a,b) &&!cmpKeys(b,a);
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

void shuffle(ThreadContext* tc){
    unsigned long int outputSize = 0;
    for (int i = 0; i < tc->p_job->getMultiThreadLevel(); i++){
        outputSize += tc->p_job->getPpersonalVectors()[i].size();
    }
    tc->p_job->addStageAtomicCounter();
    tc->p_job->resetAtomicCounterCount();
    tc->p_job->setAtomicCounterInputSize(outputSize);
    unsigned long int count = 0;
    while(count < outputSize){ // if count > outputsize, break
        int threadIdOfMax = findMaxKeyTid(tc); // find the index of the thread that contains the maximum key.
        IntermediateVec* p_currentVec = new IntermediateVec();
        IntermediatePair  currentPair = tc->p_job->getPpersonalVectors()[threadIdOfMax].back();
        // create intermidate vector
        for (int i = 0; i < tc->p_job->getMultiThreadLevel(); ++i) { // for every thread personal vector

            while( !(tc->p_job->getPpersonalVectors()[i].empty())&&  isEqualKeys(tc->p_job->getPpersonalVectors()[i].back(), currentPair)){ //get all equal key pairs
                p_currentVec->push_back(tc->p_job->getPpersonalVectors()[i].back());
                tc->p_job->getPpersonalVectors()[i].pop_back();

                count++;
            }
        }
        tc->p_job->getIntermediateVec().push_back(p_currentVec);
    }
}

//========================= reduce phase ======================================

void reduce(ThreadContext* tc){
    unsigned long int myIndex = 0;
    //the thread pick an index to work on:
    myIndex = tc->p_job->addAtomicCounter();


    while (myIndex < tc->p_job->getAtomicCounterInputSize()){

        IntermediateVec* pairs = tc->p_job->getIntermediateVec().at(myIndex);

        tc->p_job->getClient().reduce(pairs,tc);
        myIndex = tc->p_job->addAtomicCounter();
    }
}

void emit3(K3* key,V3* value,void* context){
    ThreadContext* tc = (ThreadContext*) context;

    pthread_mutex_lock(&tc->p_job->getOutputVectorMutex());
    tc->p_job->getOutputVector()->push_back(pair<K3*, V3*>(key, value));
    pthread_mutex_unlock(&tc->p_job->getOutputVectorMutex());
}

//========================= main thread logic ======================================

void* threadMainFunction(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    mapAndSort(tc);
    tc->p_job->getPAfterSortBarrier()->barrier();
    if (tc->threadID == 0) {
        shuffle(tc);
        tc->p_job->resetAtomicCounterCount();
        tc->p_job->addStageAtomicCounter();
        tc->p_job->setAtomicCounterInputSize(tc->p_job->getIntermediateVec().size());
        cout << endl << "after shuffle!" << endl;
    }
    tc->p_job->getPAfterShuffleBarrier()->barrier();

    reduce(tc);




    tc->p_job->getPAfterShuffleBarrier()->barrier();
    if (tc->threadID == 0){
        cout << endl << "after reduce!" << endl;
        printVector3(*tc->p_job->getOutputVector(), tc->threadID);
    }
    return nullptr;
}

void error_handler_function(const std::string& inputMessage){
    std::cerr<<"system error: "<<inputMessage<<std::endl;
    //TODO: free all allocate resorces
    exit(EXIT_FAILURE);
}

//========================= main exercise questions ======================================





JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    return static_cast<JobHandle> (new Job(multiThreadLevel,client,inputVec,outputVec));
}

void getJobState(JobHandle job, JobState* state){

    Job* p_job = static_cast<Job*>(job);
    state->stage = p_job->getAtomicCounterState();
    state->percentage = (100*(float)p_job->getAtomicCounterCurrent()) / (float)p_job->getAtomicCounterInputSize();
    if (state->percentage > 100.0) {
        state->percentage = 100.0;
    }
}


void waitForJob(JobHandle job){
    Job* p_job = static_cast<Job*>(job);
    if(p_job->wasWaitCalled()){
        return;
    }
    p_job->setWaitCalled(true);
    for (int i = 0; i < p_job->getMultiThreadLevel(); ++i) {
        pthread_join(p_job->getThreads()[i], NULL);
    }
}