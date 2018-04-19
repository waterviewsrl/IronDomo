/*  =====================================================================
 *  idcliapi.c - Majordomo Protocol Client API
 *  Implements the IDP/Worker spec at http://rfc.zeromq.org/spec:7.
 *  ===================================================================== */

#include "idpcliapi.h"


IDP::zmqInterruptedException zmqInterrupted;

IDP::sendFailedException sendFailed;

IDP::IDPClient::IDPClient(const std::string &zmqHost, bool verbose, int timeout, int retries)
{
    _zmqHost = zmqHost;
    _hasCurve = false;
    _verbose = verbose;
    _timeout = timeout;
    _retries = retries;
    _clientCert = nullptr;
    _client = nullptr;
    _poller = nullptr;
}

IDP::IDPClient::~IDPClient()
{
    if (_client != nullptr)
    {
        zsock_destroy (&_client);
        _client = nullptr;
    }
    if (_poller != nullptr)
    {
        zpoller_destroy(&_poller);
        _poller = nullptr;
    }
    if(_clientCert != nullptr)
    {
        zcert_destroy(&_clientCert);
        _clientCert = nullptr;
    }
}

void IDP::IDPClient::setupCurve(const std::string &clientPublic, const std::string &clientPrivate, const std::string &serverPublic)
{
    _hasCurve = true;
    _clientPublic = clientPublic;
    _clientPrivate = clientPrivate;
    _serverPublic = serverPublic;

    uint8_t sec[32];
    uint8_t pub[32];
    zmq_z85_decode(sec, _clientPrivate.c_str());
    zmq_z85_decode(pub, _clientPublic.c_str());
    _clientCert = zcert_new_from (pub, sec);
}

void IDP::IDPClient::startClient()
{
    if (_client != nullptr)
    {
        zsock_destroy (&_client);
        _client = nullptr;
    }
    if (_poller != nullptr)
    {
        zpoller_destroy(&_poller);
        _poller = nullptr;
    }
     
        
    _client = zsock_new ( ZMQ_REQ);
    _poller = zpoller_new (_client, NULL);
    if (_clientCert != nullptr)
    {
        zcert_apply (_clientCert, _client);
        zsock_set_curve_serverkey (_client, _serverPublic.c_str());
    }
    
    zsock_connect(_client, "%s", _zmqHost.c_str());
    if (_verbose)
        zclock_log ("I: connecting to broker at %s...", _zmqHost.c_str());

}

void IDP::IDPClient::setTimeout(int timeout)
{
    _timeout = timeout;
}

void IDP::IDPClient::setRetries(int retries)
{
    _retries = retries;
}

std::vector<std::string>  IDP::IDPClient::send(const std::string &service, const std::vector<std::string> &parts)
{
    std::vector<std::string> result;
    
    zmsg_t *request = zmsg_new();
    for (auto it = parts.begin(); it != parts.end(); it++)
    {
        zmsg_pushstr(request, it->c_str());
    }
    
    //  Prefix request with protocol frames
    //  Frame 1: "IDPCxy" (six bytes, IDP/Client x.y)
    //  Frame 2: Service name (printable string)
    zmsg_pushstr (request, service.c_str());
    zmsg_pushstr (request, IDPC_CLIENT);
    if (_verbose) {
        zclock_log ("I: send request to '%s' service:", service.c_str());
        zmsg_dump (request);
    }
    int retries_left = _retries;
    while (retries_left && !zctx_interrupted) {
        zmsg_t *msg = zmsg_dup (request);
        zmsg_send (&msg, _client);

       
        zsock_t *which = (zsock_t *) zpoller_wait (_poller, _timeout);

        //  If we got a reply, process it
        if (which == _client) {
            zmsg_t *msg = zmsg_recv (_client);
            if (_verbose) {
                zclock_log ("I: received reply:");
                zmsg_dump (msg);
            }
            //  We would handle malformed replies better in real code
            assert (zmsg_size (msg) >= 3);

            zframe_t *header = zmsg_pop (msg);
            assert (zframe_streq (header, IDPC_CLIENT));
            zframe_destroy (&header);

            zframe_t *reply_service = zmsg_pop (msg);
            assert (zframe_streq (reply_service, service.c_str()));
            zframe_destroy (&reply_service);

            zmsg_destroy (&request);
            char *popstr = zmsg_popstr(msg);
            while (popstr != nullptr)
            {
                result.push_back(popstr);
                free(popstr);
                popstr = zmsg_popstr(msg);
            } 
            zmsg_destroy(&msg);
            return result;
        }
        else
        if (--retries_left) {
            if (_verbose)
                zclock_log ("W: no reply, reconnecting...");
            startClient();
        }
        else {
            if (_verbose)
                zclock_log ("W: permanent error, abandoning");
            throw sendFailed;
            break;          //  Give up
        }
    }
    if (zctx_interrupted)
    {
        throw zmqInterrupted;
    }
        
    zmsg_destroy (&request);


    return result;
}


