/*  =====================================================================
 *  idpwrkapi.h - Irondomo Protocol Worker API
 *  ===================================================================== */

#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "czmq.h"
#include "idp_common.h"



namespace IDP
{

class IDPWorker
{
  public:
    IDPWorker(const std::string &zmqHost, const std::string &service, bool verbose = false, int timeout = 2500, int retries = 3, bool unique=false);
    ~IDPWorker();

    void setupCurve(const std::string &workerPublic, const std::string &workerPrivate, const std::string &serverPublic);
    void startWorker();
    void setHeartbeat(int heartbeat);
    void setRetries(int retries);
    void setReconnectTimeout(int reconnect_timeout);

    void loop(void);
    

  private:
    virtual std::vector<std::pair<unsigned char *, size_t>> callback(const std::vector<std::pair<unsigned char *, size_t>> &parts) = 0;
    void send_to_broker (char const *command, char const *option, zmsg_t *msg);
    zmsg_t *receive (zmsg_t **reply_p);
    
    std::string _zmqHost;
    std::string _service;
    std::string _workerPublic;
    std::string _workerPrivate;
    std::string _serverPublic;
    std::string _identity;
    bool _hasCurve;
    zsock_t *_worker; //  Socket to broker
    zpoller_t *_poller;
    zcert_t *_workerCert;
    int _verbose; //  Print activity to stdout
    int _heartbeat; //  Request timeout
    int _retries; //  Max Retries
    int _liveness; // Remaining Retries
    int _reconnect_timeout; // Waiting time before reconnecting
    uint64_t _heartbeat_at;      //  When to send HEARTBEAT
    bool _expect_reply;
    zframe_t *_reply_to_clear;
    zframe_t *_reply_to_curve;
};
}


