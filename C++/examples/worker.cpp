#include "idpwrkapi.h"
#include <string>
#include <vector>
#include <iostream>
#include <iomanip>

class worker : public IDP::IDPWorker::IDPWorker
{
private:
    std::vector<std::pair<unsigned char *, size_t>> callback(const std::vector<std::pair<unsigned char *, size_t>> &parts) override;

public:
worker(const std::string &zmqHost, const std::string &service, bool verbose = false, int timeout = 2500, int retries = 3, bool unique=false) : IDP::IDPWorker::IDPWorker(zmqHost, service, verbose, timeout, retries, unique) {}
};

std::vector<std::pair<unsigned char *, size_t>> worker::callback(const std::vector<std::pair<unsigned char *, size_t>> &parts)
{
    std::vector<std::pair<unsigned char *, size_t>> reply_vector;
    /*for (auto it = parts.begin(); it != parts.end(); it++)
    {
        std::cout << "Parte: ";
        for (int i=0; i < it->second; i++)
            std::cout << " 0x" << std::hex << std::setfill('0') << std::setw(2) <<  (int)it->first[i];
        std::cout << std::endl; 
        reply_vector.push_back(*it);
    }*/
    std::string echo("echo");
    std::pair<unsigned char *, size_t> p((unsigned char*)strdup(echo.c_str()), echo.size());
    reply_vector.push_back(p);


    return reply_vector;
}

int main()
{

    std::string broker("tcp://127.0.0.1:5000");
    std::string service("echo");
    worker *w = new worker(broker, service, true);

    w->startWorker();

    w->loop();

}
