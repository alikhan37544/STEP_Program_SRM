#include <string>
#include <vector>
#include <iostream>

class Paradigm
{
public:
    virtual std::string getType() = 0;
    virtual ~Paradigm() = default;
};

class OOP: public Paradigm
{
public:
    std::string getType() override
    {
        return "Object-Oriented";
    }
};

class Procedural: public Paradigm
{
public:
    std::string getType() override
    {
        return "Procedural";
    }
};

class AOP: public Paradigm
{
public:
    std::string getType() override
    {
        return "AOP";
    }
};

class ParadigmFactory {
public:
    static std::vector<Paradigm*> createParadigms(const std::string& langName) {
        std::vector<Paradigm*> paradigms;
        if (langName == "C") {
            paradigms.push_back(new Procedural());
        } else if (langName == "CPP") {
            paradigms.push_back(new Procedural());
            paradigms.push_back(new OOP());
        } else if (langName == "Java") {
            paradigms.push_back(new OOP());
            paradigms.push_back(new AOP());
        }
        return paradigms;
    }
};

class ProgrammingLanguage {
protected:
    std::string name;
    std::vector<Paradigm*> paradigms;

public:
    ProgrammingLanguage(const std::string& name) : name(name) {
        paradigms = ParadigmFactory::createParadigms(name);
    }

    virtual std::string getName() const { return name; }

    virtual std::string getType() const {
        std::string result;
        for (size_t i = 0; i < paradigms.size(); ++i) {
            result += paradigms[i]->getType();
            if (i < paradigms.size() - 1) {
                result += ", ";
            }
        }
        return result;
    }

    virtual ~ProgrammingLanguage() {
        for (Paradigm* p : paradigms) {
            delete p;
        }
    }

};


class C : public ProgrammingLanguage {
public:
    C() : ProgrammingLanguage("C") {}
};

class CPP : public ProgrammingLanguage {
public:
    CPP() : ProgrammingLanguage("CPP") {}
};

class Java : public ProgrammingLanguage {
public:
    Java() : ProgrammingLanguage("Java") {}
};


ProgrammingLanguage* createLanguage(const std::string& name) {
    if (name == "C") return new C();
    if (name == "CPP") return new CPP();
    if (name == "Java") return new Java();
    return nullptr;
}

int main() {
    std::vector<ProgrammingLanguage*> languages;

    languages.push_back(createLanguage("C"));
    languages.push_back(createLanguage("CPP"));
    languages.push_back(createLanguage("Java"));

    for (ProgrammingLanguage* lang : languages) {
        std::cout << "Language: " << lang->getName() << std::endl;
        std::cout << "Paradigm(s): " << lang->getType() << std::endl;
        std::cout << "--------------------------" << std::endl;
    }

    // Clean up
    for (ProgrammingLanguage* lang : languages) {
        delete lang;
    }

    return 0;
}