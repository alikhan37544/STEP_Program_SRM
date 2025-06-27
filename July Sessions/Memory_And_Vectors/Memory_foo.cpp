#include <iostream>
using namespace std;

class DemoClass
{
public:
    void foo()
    {
        cout<<"foo()"<<endl;
    }
    
    void foo(int x)
    {
        cout<<"foo(int)"<<endl;
    }
    
    void foo(int x,int y)
    {
        cout<<"foo(int,int)"<<endl;
    }
};


int main()
{
    DemoClass dc;
    
    dc.foo();
    dc.foo(10);
    dc.foo(10,20);
    
    return 0;
}