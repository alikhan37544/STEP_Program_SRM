#include <iostream>
using namespace std;

// char* str;


// int main()
// {
//     const char* s;
//     s = "Hello Smart SRM";
    
//     s++;
//     cout << s << endl; 
//     cout << &s << endl;

//     s--;
//     cout << s << endl;
//     cout << &s << endl;
//     s--;
//     cout << s << endl;
//     cout << &s << endl;
//     return 0;
// }


// #include <iostream>
// using namespace std;

// class Vector
// {
//     int* p;
//     int sz;
// public:
//     int* begin() { return p; }
//     int* end() { return p + sz; }



//     // Default contsturctors
//     Vector() : sz(0), p(nullptr) {}

//     // Parameters 
//     Vector(int size) : sz(size), p(new int[size]()) {}


//     Vector(int size, int value) : sz(size), p(new int[size]) {
//         for(int i = 0; i < sz; ++i) p[i] = value;
//     }

//     // Copy
//     Vector(int size, const int* arr) : sz(size), p(new int[size]) {
//         for(int i = 0; i < sz; ++i) p[i] = arr[i];
//     }

//     // Popping using the index from the vector
//     int& operator[](int index) { return p[index]; }


//     /*
//     There 
//     */

   

//     ~Vector() { delete[] p; }
// };


// void display(int* F, int* L)
// {
//     for( ; F!= L; ++F)
//         cout<<*F<<"\t";
//     cout<<endl;
// }

// int main()
// {
//     int arr[] = { 10,20, 30, 40, 50 };
//     Vector  v1,         // size is 0,
//             v2(10),     // size is 10, all vales are 0
//             v3(10,5),    // size is 10, all vales are 5
//             v4(10,arr);
            
//     display(v1.begin(), v1.end());
//     display(v2.begin(), v2.end());
//     display(v3.begin(), v3.end());
//     display(v4.begin(), v4.end());

//     return 0;
// }



// #include <iostream>
// #include <vector>
// #include <algorithm>
// using namespace std;

// template<class T>
// class Vector
// {
//     T* p;
//     int sz;
// public:
//     T* begin()
//     {
//         return p;
//     }
//     T* end()
//     {
//         return p + sz;
//     }
    
//     Vector(): sz(0), p(NULL) {}
    
//     Vector(Vector &obj) : sz(obj.sz)
//     {
//         p = new T[sz];
        
//         for(int i=0; i<sz; ++i)
//         {
//             p[i] = obj.p[i];
//         }
        
//     }
    
//     Vector& operator =(Vector &obj)
//     {
//         sz = obj.sz;
//         p = new T[sz];
        
//         for(int i=0; i<sz; ++i)
//         {
//             p[i] = obj.p[i];
//         }
        
//         return *this;
        
//     }
    
//     Vector(int _sz, T fill=T()): sz(_sz) 
//     {
//         p = new T[sz];
        
//         for (int i=0; i<sz; ++i)
//         {
//            p[i] = fill; 
//         }
//     }
    
//     Vector(T* F, T* L): sz(L - F) 
//     {
//         p = new T[sz];
        
//         for (int i=0; i<sz; ++i) 
//         {
//             p[i] = F[i];
//         }
//     }
    
//     ~Vector() 
//     {
//         cout<<"Address of p: "<<p<<endl;
//         delete[] p;
//         sz = 0;
//         p = NULL;
//     }
    
// };
// template<class II>
// void display(II F, II L)
// {
//     for( ; F!= L; ++F)
//         cout<<*F<<"\t";
//     cout<<endl;
// }

// int main()
// {
//     int arr[] = { 10,20, 30, 40, 50, 60, 70,80, 90, 100 };
//     Vector<int> v1,         // size is 0,
//                 v2(10),         // size is 10, all vales are 0
//                 v3(10,5),       // size is 10, all vales are 5
//                 v4(arr,arr+10),   // size is 10, store vales of the arr
//                 v5(v3);
            
//     v1 = v5;
    
//     display(v1.begin(), v1.end());
//     display(v2.begin(), v2.end());
//     display(v3.begin(), v3.end());
//     display(v4.begin(), v4.end());
//     display(v5.begin(), v5.end());
    
            
//     return 0;
// }



#include <iostream>
#include <vector>
#include <algorithm>
using namespace std;

template<class T>
class Vector
{
    T* p;
    int sz;
public:
    T* begin()
    {
        return p;
    }
    T* end()
    {
        return p + sz;
    }
    
    // This part of the code basically is a constructor for the following scenario : 
    // Creating an empty vector with 0 values.
    // This is also the default constructor
    Vector(): sz(0), p(NULL) {}
    

    // This part of the code is a copy constructor 
    Vector(Vector &obj) : sz(obj.sz)
    {
        p = new T[sz];
        
        for(int i=0; i<sz; ++i)
        {
            p[i] = obj.p[i];
        }
        
    }
    
    Vector& operator =(Vector &obj)
    {
        sz = obj.sz;
        p = new T[sz];
        
        for(int i=0; i<sz; ++i)
        {
            p[i] = obj.p[i];
        }
        
        return *this;
        
    }
    
    Vector(int _sz, T fill=T()): sz(_sz) 
    {
        p = new T[sz];
        
        for (int i=0; i<sz; ++i)
        {
           p[i] = fill; 
        }
    }
    
    Vector(T* F, T* L): sz(L - F) 
    {
        p = new T[sz];
        
        for (int i=0; i<sz; ++i) 
        {
            p[i] = F[i];
        }
    }
    
    ~Vector() 
    {
        cout<<"Address of p: "<<p<<endl;
        delete[] p;
        sz = 0;
        p = NULL;
    }
    
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
    int arr[] = { 10,20, 30, 40, 50, 60, 70,80, 90, 100 };
    Vector<int> v1,         // size is 0,
                v2(10),         // size is 10, all vales are 0
                v3(10,5),       // size is 10, all vales are 5
                v4(arr,arr+10),   // size is 10, store vales of the arr
                v5(v3);
            
    v1 = v5;
    
    display(v1.begin(), v1.end());
    display(v2.begin(), v2.end());
    display(v3.begin(), v3.end());
    display(v4.begin(), v4.end());
    display(v5.begin(), v5.end());
    
            
    return 0;
}