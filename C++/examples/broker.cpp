#include "idpbroker.h"
#include <string>
#include <vector>
#include <iostream>
#include <iomanip>


int main()
{

    std::string broker("tcp://127.0.0.1:5000");
    std::string broker_curve("tcp://127.0.0.1:5001");
    //std::string broker_curve("");

    /*zcert_t * c = zcert_new();

    std::string public_key(zcert_public_txt(c));
    std::string secret_key(zcert_secret_txt(c));*/
    std::string public_key(".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr");
    std::string secret_key("3vup%:I!lF>^QWT@[[g]dwa>1:(B-^3RWw^7tIMf");

    std::cout << "Server PUBLIC_KEY is: " << public_key << std::endl;
    std::cout << "Server SECRET_KEY is: " << secret_key << std::endl;

    std::pair<std::string, std::string> *certs = new std::pair<std::string, std::string>(public_key, secret_key);

    std::string service("echo");
    IDP::IDPBroker *b = new IDP::IDPBroker(broker, broker_curve, certs, "asdasd", false, true);

    b->loop();

    delete b;
    delete certs;

}
