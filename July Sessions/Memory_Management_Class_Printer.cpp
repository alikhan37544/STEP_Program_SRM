#include <iostream>
using namespace std;

// this is a printer on the network and shared. Hence has to be managed.

class PrinterMgr;

class Printer
{
    bool inUse;
    Printer() : inUse(false) { }
    
    friend class PrinterMgr;
public:
    void printDocument(string name) const
    {
        cout<<"Printing "<<name<<endl;
    }
    void setInUse(bool inUse)
    {
        this->inUse = inUse;
    }
    bool isAvailable(){
        return inUse;
    }
    void changeConfig() {
        // changes the settings...
    }
};

class PrinterMgr
{
    Printer* printer;
public:
    PrinterMgr() 
    { 
        printer = new Printer; 
    }
    Printer* getPrinter()
    {
        const Printer* p = NULL;
        
        if(printer->isAvailable())
        {   
            p->setInUse(true);
            p = printer;
        }
            
        return p;
    }
};

void client(const Printer* printer)
{
    // printer->changeConfig();
    
    printer->printDocument("Sample.cpp");
}

int main()
{    
    PrinterMgr *mgr = new PrinterMgr;
    
    Printer* printer =  NULL; //new Printer;
        mgr->getPrinter();
    
    client(printer);
    
    return 0;
}