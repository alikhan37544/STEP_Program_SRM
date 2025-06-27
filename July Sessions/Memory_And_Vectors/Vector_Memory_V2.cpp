#include <iostream>
#include <vector>
#include <algorithm>
#include <cstring>
using namespace std;

// This abstract class defines the interface for the implementation side of the bridge.
// It allows different storage and memory management strategies for the Vector class.
template<class T>
class VectorImpl {
public:
    virtual ~VectorImpl() = default;

    // Allocate memory for the vector (not used in this implementation, handled by Vector)
    virtual void allocate(unsigned int sz, int capacity) = 0;

    // Initialize the vector with a fill value
    virtual void initialize(T val, T* F, T* E) = 0;

    // Initialize the vector by copying from another array
    virtual void initialize(T* _F, T* F, T* E) = 0;

    // Release memory/resources held by the vector
    virtual void release(T* F, T* E) = 0;

    // Add an element to the end of the vector, resizing if necessary
    virtual void pushBack(T elem, T*& F, T*& E, T*& L, int& capacity) = 0;
};

// This class provides the default implementation for the VectorImpl interface.
// It works for any type T that can be copied and deleted with delete[].
template<class T>
class GenericVectorImpl : public VectorImpl<T> {
public:
    void allocate(unsigned int sz, int capacity) override {
        // Allocation is handled by the Vector class itself.
    }
    
    // Fill the range [F, E) with the value val
    void initialize(T val, T* F, T* E) override {
        int sz = E - F;
        for(int i = 0; i < sz; ++i)
            F[i] = val;
    }
    
    // Copy elements from _F to [F, E)
    void initialize(T* _F, T* F, T* E) override {
        int sz = E - F;
        for(int i = 0; i < sz; ++i)
            F[i] = _F[i];
    }
    
    // Release memory for the array
    void release(T* F, T* E) override {
        delete[] F;
    }
    
    // Add an element to the end, resizing if needed
    void pushBack(T elem, T*& F, T*& E, T*& L, int& capacity) override {
        if(capacity == 0){
            capacity = 2;
            F = new T[capacity];
            E = F;
        }
        if(E - F == capacity){
            int sz = capacity;
            capacity = 2 * capacity;
            T* _F = new T[capacity];
            for(int i = 0; i < sz; ++i){
                _F[i] = F[i];
            }
            delete[] F;
            F = _F;
            E = F + sz;
        }
        *E = elem;
        ++E;
        L = E - 1;
    }
};

// ==========================
// Specialized Implementation for char*
// ==========================
// This specialization handles deep copying and memory management for arrays of C-strings.
template<>
class GenericVectorImpl<char*> : public VectorImpl<char*> {
public:
    void allocate(unsigned int sz, int capacity) override {
        // Allocation is handled by the Vector class itself.
    }
    
    // Fill [F, E) with deep copies of val
    void initialize(char* val, char** F, char** E) override {
        int sz = E - F;
        for(int i = 0; i < sz; ++i) {
            if(F[i]) delete[] F[i]; // Free old memory if present
            F[i] = new char[strlen(val) + 1];
            strcpy(F[i], val);
        }
    }
    
    // Copy C-strings from _F to [F, E)
    void initialize(char** _F, char** F, char** E) override {
        int sz = E - F;
        for(int i = 0; i < sz; ++i) {
            if(F[i]) delete[] F[i]; // Free old memory if present
            F[i] = new char[strlen(_F[i]) + 1];
            strcpy(F[i], _F[i]);
        }
    }
    
    // Free all C-strings and the array itself
    void release(char** F, char** E) override {
        for(int i = 0; i < E - F; ++i) {
            delete[] F[i];
        }
        delete[] F;
    }
    
    // Add a new C-string to the end, resizing if needed
    void pushBack(char* elem, char**& F, char**& E, char**& L, int& capacity) override {
        if(capacity == 0){
            capacity = 2;
            F = new char*[capacity];
            for(int i = 0; i < capacity; ++i) F[i] = nullptr;
            E = F;
        }
        if(E - F == capacity){
            int sz = capacity;
            capacity = 2 * capacity;
            char** _F = new char*[capacity];
            for(int i = 0; i < capacity; ++i) _F[i] = nullptr;
            
            for(int i = 0; i < sz; ++i){
                _F[i] = new char[strlen(F[i]) + 1];
                strcpy(_F[i], F[i]);
                delete[] F[i];
            }
            delete[] F;
            F = _F;
            E = F + sz;
        }
        *E = new char[strlen(elem) + 1];
        strcpy(*E, elem);
        ++E;
        L = E - 1;
    }
};

// ==========================
// Vector Class (Abstraction)
// ==========================
// This class provides a vector-like container using the Bridge pattern.
// It delegates memory management and storage to a VectorImpl implementation.
template<class T>
class Vector {
public:
    // Default constructor
    Vector() : F(nullptr), L(nullptr), E(nullptr), capacity(0), impl(new GenericVectorImpl<T>()) { }
    
    // Construct with size and fill value
    Vector(unsigned int sz, T fill = T()) : capacity(sz + 1), impl(new GenericVectorImpl<T>()) { 
        allocate(sz);
        initialize(fill);
    }
    
    // Construct from array [_F, _E)
    Vector(T* _F, T* _E): capacity(_E - _F + 1), impl(new GenericVectorImpl<T>()) { 
        allocate(_E - _F);
        initialize(_F);
    }
    
    // Copy constructor (deep copy)
    Vector(const Vector<T>& v) : capacity(v.capacity), impl(new GenericVectorImpl<T>()) { 
        allocate(v.E - v.F);
        initialize(v.F);
    }
    
    // Assignment operator (deep copy)
    Vector& operator=(const Vector &obj) {
        if(this != &obj){
            release();
            capacity = obj.capacity;
            allocate(obj.E - obj.F);
            initialize(obj.F);
        }
        return *this;
    }
    
    // Add an element to the end
    Vector& push_back(T elem){
        impl->pushBack(elem, F, E, L, capacity);
        return *this;
    }
    
    // Destructor: release memory and implementation
    ~Vector() {
        release();
        delete impl;
    }
    
    // Access the first element
    T& front() { return *F; }
    // Access the last element
    T& back() { return *L; }
    
    // Access element by index (no bounds checking)
    T& operator[](int index) {
        return F[index];
    }
    
    // Access element by index (no bounds checking)
    T& at(int index) {
        return F[index];
    }
    
    // Get the number of elements
    int size() { return E - F; }
    
    // Get pointer to the beginning
    T* begin() { return F; }
    // Get pointer to the end (one past the last element)
    T* end() { return E; }

protected:
    // Allocate memory for the vector
    void allocate(unsigned int sz) {
        if constexpr (std::is_same_v<T, char*>) {
            // For char*, initialize all pointers to nullptr
            F = new T[capacity];
            for(int i = 0; i < capacity; ++i) {
                static_cast<char**>(static_cast<void*>(F))[i] = nullptr;
            }
        } else {
            F = new T[capacity];
        }
        E = F + sz;
        L = E - 1;
    }
    
    // Initialize with a fill value
    void initialize(T val) {
        impl->initialize(val, F, E);
    }
    
    // Initialize by copying from another array
    void initialize(T* _F) {
        impl->initialize(_F, F, E);
    }
    
    // Release memory/resources
    void release() {
        if(F) {
            impl->release(F, E);
            F = nullptr;
            L = nullptr;
            E = nullptr;
            capacity = 0;
        }
    }

private:
    int capacity;           // Allocated capacity of the array
    T* F;                   // Pointer to the first element
    T* L;                   // Pointer to the last element
    T* E;                   // Pointer to one past the last element
    VectorImpl<T>* impl;    // Pointer to the implementation (Bridge)
};

// ==========================
// Utility function to display a range
// ==========================
template<class II>
void display(II F, II L) {
    for( ; F != L; ++F)
        cout << *F << "\t";
    cout << endl;
}

// ==========================
// Main function demonstrating usage
// ==========================
int main() {
    // Array of C-strings to initialize the vector
    char* arr[] = { "STL ", "containers ", "are ", "fully ", "optimized " };
    
    // Create empty vector of char*
    Vector<char*> v1;
    // Create vector from array
    Vector<char*> v2(arr, arr + 5);
    // Copy constructor
    Vector<char*> v3(v2);
            
    // Assignment operator
    v1 = v3;
    
    // Add more strings to v1
    v1.push_back("for ");
    v1.push_back("time ");
    v1.push_back("and ");
    v1.push_back("space ");
    
    // Display all vectors
    display(v1.begin(), v1.end());
    display(v2.begin(), v2.end());
    display(v3.begin(), v3.end());

    cout << "Change the first string: " << endl;
    char s1 = "";
    cin >> s1;
    
    // Replace the first string in v1 with user input
    delete[] v1[0]; // Free old memory
    v1[0] = new char[strlen(s1) + 1];
    strcpy(v1[0], s1);

    // Display updated v1
    display(v1.begin(), v1.end());
    
    return 0;
}