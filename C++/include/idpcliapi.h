/*  =====================================================================
 *  idpcliapi.h - Irondomo Protocol Client API
 *  ===================================================================== */

#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "czmq.h"
#include "idp_common.h"

namespace IDP
{

class IDPClient
{
  public:
    IDPClient(const std::string &zmqHost, const std::string &identity, bool verbose = false, int timeout = 2500, int retries = 3);
    ~IDPClient();

    void setupCurve(const std::string &clientPublic, const std::string &clientPrivate, const std::string &serverPublic);
    void startClient();
    void setTimeout(int timeout);
    void setRetries(int retries);
    std::vector<std::string> send(const std::string &service, const std::vector<std::string> &parts);
    std::vector<std::string> sendraw(const std::string &service, const std::vector<std::pair<unsigned char *, size_t>> &parts);


  private:
    std::string _zmqHost;
    std::string _clientPublic;
    std::string _clientPrivate;
    std::string _serverPublic;
    std::string _identity;
    bool _hasCurve;
    zsock_t *_client; //  Socket to broker
    zpoller_t *_poller;
    zcert_t *_clientCert;
    int _verbose; //  Print activity to stdout
    int _timeout; //  Request timeout
    int _retries; //  Request retries
};
}
