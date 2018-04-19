/*  =====================================================================
 *  idcliapi.h - Majordomo Protocol Client API
 *  Implements the IDP/Worker spec at http://rfc.zeromq.org/spec:7.
 *  ===================================================================== */

#ifndef __IDPCLIAPI_H_INCLUDED__
#define __IDPCLIAPI_H_INCLUDED__

#include <string>
#include <vector>
#include <iostream>

#include "czmq.h"
#include "idp_common.h"

namespace IDP
{
class zmqInterruptedException: public std::exception
{
  virtual const char* what() const throw()
  {
    return "ZeroMQ context was interrupted";
  }
};

class sendFailedException: public std::exception
{
  virtual const char* what() const throw()
  {
    return "Request Failed";
  }
};

class IDPClient
{
  public:
    IDPClient(const std::string &zmqHost, bool verbose = false, int timeout = 2500, int retries = 3);
    ~IDPClient();

    void setupCurve(const std::string &clientPublic, const std::string &clientPrivate, const std::string &serverPublic);
    void startClient();
    void setTimeout(int timeout);
    void setRetries(int retries);
    std::vector<std::string> send(const std::string &service, const std::vector<std::string> &parts);

  private:
    std::string _zmqHost;
    std::string _clientPublic;
    std::string _clientPrivate;
    std::string _serverPublic;
    bool _hasCurve;
    zsock_t *_client; //  Socket to broker
    zpoller_t *_poller;
    zcert_t *_clientCert;
    int _verbose; //  Print activity to stdout
    int _timeout; //  Request timeout
    int _retries; //  Request retries
};
}

#endif
