#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;

class LargeObject
{
    int arr[1000];  
};

template<class T>
class Vector
{
public:
    Vector() : F(NULL), L(NULL), E(NULL), capacity(0) { }
    Vector(unsigned int sz, T fill = T()) : capacity(sz+1) 
    { 
        F = new T[capacity];
        E = F + sz;
        L = E - 1;
        
        for(int i=0;i<sz;++i)
            F[i] = fill;
    }
    
    Vector(T* _F, T* _E): capacity(_E - _F + 1)
    { 
        int sz = _E - _F;
        F = new T[capacity];
        E = F + sz;
        L = E - 1;
        
        for(int i=0;i<sz;++i)
            F[i] = _F[i];
    }
    
    Vector(Vector<T>& v) 
    { 
        int sz = v.E - v.F;
        capacity = v.capacity;
        F = new T[capacity];
        E = F + sz;
        L = E - 1;
        
        for(int i=0;i<sz;++i)
            F[i] = v.F[i];
    }
    
    // operator= ()
    Vector& operator =(const Vector &obj) {
    if(this!=&obj){
        delete[] F;
        int sz = obj.E-obj.F;
        capacity = obj.capacity;
        F = new T[capacity];
        for(int i=0; i<sz; ++i) {
            F[i] = obj.F[i];
        }
        E = F+sz;
        L = E-1;
    }
    return *this;
    }
    
    // push_back() 
    Vector& push_back(T elem){
        if(capacity == 0){
            capacity = 2;
            F = new T[capacity];
            E = F;
        }
        if(E-F == capacity){
            int sz = capacity;
            capacity = 2*capacity;
            T* _F = new T[capacity];
            for(int i=0; i<sz; ++i){
                _F[i] = F[i];
            }
            delete[] F;
            F = _F;
            E = F+sz;
            *E = elem;
            ++E;
            L = E-1;
            return *this;
        }
        *E = elem;
        ++E;
        L = E-1;
        return *this;
    }
    
    T& front() { return *F; }
    T& back() { return *L; }
    
    // pop_back()
    
    T& operator[](int index)
    {
        return F[index];
    }
    
    T& at(int index)
    {
        return F[index];
    }
    
    int size() {  return E- F; }
    
    T* begin() { return F; }
    T* end() { return E; }
    
private:
    int capacity;
    T* F;
    T* L;
    T* E;
};


template<class II>
void display(II F, II L)
{
    for( ; F!= L; ++F)
        cout<<*F<<"\t";
    cout<<endl;
}

int main()
{
    // A: Original data array
    char* arr[] = {"STL ", "containers ", "are ", "Fully ", "Optimized"};
    int n = sizeof(arr) / sizeof(arr[0]);
    

    // Convert array to Vector<char*>
    Vector<char*> vA(arr, arr + n);

    // B: Array of pointers, each pointing to an element in vA
    char** b = new char*[n];
    for(int i = 0; i < n; ++i) {
        b[i] = vA[i]; // Each b[i] points to vA[i]
    }

    // C: Pointer to the array of pointers (B)
    char*** C = &b;

    // Print values and addresses for comparison
    cout << "Original array A (arr):" << endl;
    for(int i = 0; i < n; ++i) {
        cout << "arr[" << i << "] value: " << arr[i]
             << ", address: " << static_cast<void*>(arr[i]) << endl;
    }

    cout << "\nVector vA:" << endl;
    for(int i = 0; i < vA.size(); ++i) {
        cout << "vA[" << i << "] value: " << vA[i]
             << ", address: " << static_cast<void*>(vA[i]) << endl;
    }

    cout << "\nArray of pointers B:" << endl;
    for(int i = 0; i < n; ++i) {
        cout << "b[" << i << "] value: " << b[i]
             << ", address: " << static_cast<void*>(b[i]) << endl;
    }

    cout << "\nPointer to array of pointers C:" << endl;
    cout << "C (address of b): " << C << endl;
    cout << "*C (b): " << *C << endl;
    cout << "**C (b[0]): " << **C << endl;

    // Compare pointers
    cout << "\nComparisons:" << endl;
    for(int i = 0; i < n; ++i) {
        cout << "arr[" << i << "] == vA[" << i << "]? " << (arr[i] == vA[i] ? "Yes" : "No") << endl;
        cout << "b[" << i << "] == vA[" << i << "]? " << (b[i] == vA[i] ? "Yes" : "No") << endl;
    }

    // Clean up
    delete[] b;

    return 0;
}


// Do a deep copy of the vector and make sure that the original vector is not affected by changes made to the copied vector.
// The code should also handle char input, and according to that we should be able to figure out them.

