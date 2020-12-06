/*  =====================================================================
 *  idcliapi.c - Irondomo Protocol Worker API
 *  Implements the IDP/Worker spec at http://rfc.zeromq.org/spec:7.
 *  ===================================================================== */

#include "idpwrkapi.h"
#include <iostream>
#include <vector>
#include <random>
#include <functional> //for std::function
#include <algorithm>  //for std::generate_n

IDP::zmqInterruptedException zmqInterrupted;
IDP::sendFailedException sendFailed;

typedef std::vector<char> char_array;

char_array charset()
{
    //Change this to suit
    return char_array(
        {'0', '1', '2', '3', '4',
         '5', '6', '7', '8', '9',
         'A', 'B', 'C', 'D', 'E', 'F',
         'G', 'H', 'I', 'J', 'K',
         'L', 'M', 'N', 'O', 'P',
         'Q', 'R', 'S', 'T', 'U',
         'V', 'W', 'X', 'Y', 'Z',
         'a', 'b', 'c', 'd', 'e', 'f',
         'g', 'h', 'i', 'j', 'k',
         'l', 'm', 'n', 'o', 'p',
         'q', 'r', 's', 't', 'u',
         'v', 'w', 'x', 'y', 'z'});
};

// given a function that generates a random character,
// return a string of the requested length
std::string random_string(size_t length, std::function<char(void)> rand_char)
{
    std::string str(length, 0);
    std::generate_n(str.begin(), length, rand_char);
    return str;
}

IDP::IDPWorker::IDPWorker(const std::string &zmqHost, const std::string &service, bool verbose, int heartbeat, int retries, bool unique)
{
    _zmqHost = zmqHost;
    _service = service;

    //0) create the character set.
    //   yes, you can use an array here,
    //   but a function is cleaner and more flexible
    const auto ch_set = charset();

    //1) create a non-deterministic random number generator
    std::default_random_engine rng(std::random_device{}());

    //2) create a random number "shaper" that will give
    //   us uniformly distributed indices into the character set
    std::uniform_int_distribution<> dist(0, ch_set.size() - 1);

    //3) create a function that ties them together, to get:
    //   a non-deterministic uniform distribution from the
    //   character set of your choice.
    auto randchar = [ch_set, &dist, &rng]() { return ch_set[dist(rng)]; };

    _identity = _service + "_" + (unique ? "0" : random_string(4, randchar));
    _hasCurve = false;
    _verbose = verbose;
    _heartbeat = heartbeat;
    _retries = retries;
    _reconnect_timeout = 2500;
    _workerCert = nullptr;
    _worker = nullptr;
    _poller = nullptr;
    _expect_reply = 0;
}

IDP::IDPWorker::~IDPWorker()
{
    if (_worker != nullptr)
    {
        zsock_destroy(&_worker);
        _worker = nullptr;
    }
    if (_poller != nullptr)
    {
        zpoller_destroy(&_poller);
        _poller = nullptr;
    }
    if (_workerCert != nullptr)
    {
        zcert_destroy(&_workerCert);
        _workerCert = nullptr;
    }
}

void IDP::IDPWorker::setupCurve(const std::string &workerPublic, const std::string &workerPrivate, const std::string &serverPublic)
{
    _hasCurve = true;
    _workerPublic = workerPublic;
    _workerPrivate = workerPrivate;
    _serverPublic = serverPublic;

    uint8_t sec[32];
    uint8_t pub[32];
    zmq_z85_decode(sec, _workerPrivate.c_str());
    zmq_z85_decode(pub, _workerPublic.c_str());
    _workerCert = zcert_new_from(pub, sec);
}

unsigned int connCnt = 0;

void IDP::IDPWorker::startWorker()
{
    if (_worker != nullptr)
    {
        zsock_destroy(&_worker);
        _worker = nullptr;
    }
    if (_poller != nullptr)
    {
        zpoller_destroy(&_poller);
        _poller = nullptr;
    }

    _worker = zsock_new(ZMQ_DEALER);
    zsock_set_identity(_worker, _identity.c_str());

    _poller = zpoller_new(_worker, NULL);
    if (_workerCert != nullptr)
    {
        zcert_apply(_workerCert, _worker);
        zsock_set_curve_serverkey(_worker, _serverPublic.c_str());
    }

    zsock_connect(_worker, "%s", _zmqHost.c_str());
    if (_verbose)
        zclock_log("I: connecting to broker at %s...", _zmqHost.c_str());

    this->send_to_broker(IDPW_READY, (char *)_service.c_str(), NULL);

    _liveness = _retries;
    _heartbeat_at = zclock_time() + _heartbeat;
}

void IDP::IDPWorker::setHeartbeat(int heartbeat)
{
    _heartbeat = heartbeat;
}

void IDP::IDPWorker::setRetries(int retries)
{
    _retries = retries;
}

void IDP::IDPWorker::setReconnectTimeout(int reconnect_timeout)
{
    _reconnect_timeout = reconnect_timeout;
}

void IDP::IDPWorker::send_to_broker(char const *command, char const *option, zmsg_t *msg)
{
    msg = msg ? zmsg_dup(msg) : zmsg_new();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr(msg, option);
    zmsg_pushstr(msg, command);
    zmsg_pushstr(msg, IDPW_WORKER);
    zmsg_pushstr(msg, "");

    if (_verbose)
    {
        zclock_log("I: sending %s to broker",
                   idps_commands[(int)*command]);
        zmsg_dump(msg);
    }
    zmsg_send(&msg, _worker);
}

//  ---------------------------------------------------------------------
//  Send reply, if any, to broker and wait for next request.

zmsg_t *IDP::IDPWorker::receive(zmsg_t **reply_p)
{
    //  Format and send the reply if we were provided one
    assert(reply_p);
    zmsg_t *reply = *reply_p;
    assert(reply || !_expect_reply);
    if (reply)
    {
        if (_reply_to_clear)
        {
            zmsg_wrap(reply, _reply_to_clear);
            this->send_to_broker(IDPW_REPLY, NULL, reply);
        }
        else if (_reply_to_curve)
        {
            zmsg_wrap(reply, _reply_to_curve);
            this->send_to_broker(IDPW_REPLY_CURVE, NULL, reply);
        }
        else
        {
            zclock_log("E: MISSING REPLY!");
        }

        zmsg_destroy(reply_p);
    }
    _expect_reply = 1;

    while (true)
    {

        zsock_t *which = (zsock_t *)zpoller_wait(_poller, _heartbeat * ZMQ_POLL_MSEC);

        if (which == NULL)
        {
            if (zpoller_terminated(_poller))
            {
                break; //  Interrupted
            }
        }

        if (which == _worker)
        {
            zmsg_t *msg = zmsg_recv(_worker);
            if (!msg)
                break; //  Interrupted
            if (_verbose)
            {
                zclock_log("I: received message from broker:");
                zmsg_dump(msg);
            }
            _liveness = _retries;

            //  Don't try to handle errors, just assert noisily
            assert(zmsg_size(msg) >= 3);

            zframe_t *empty = zmsg_pop(msg);
            assert(zframe_streq(empty, ""));
            zframe_destroy(&empty);

            zframe_t *header = zmsg_pop(msg);
            assert(zframe_streq(header, IDPW_WORKER));
            zframe_destroy(&header);

            zframe_t *command = zmsg_pop(msg);
            if (zframe_streq(command, IDPW_REQUEST) || zframe_streq(command, IDPW_REQUEST_CURVE))
            {
                //  We should pop and save as many addresses as there are
                //  up to a null part, but for now, just save one...
                _reply_to_clear = zframe_streq(command, IDPW_REQUEST) ? zmsg_unwrap(msg) : NULL;
                _reply_to_curve = zframe_streq(command, IDPW_REQUEST_CURVE) ? zmsg_unwrap(msg) : NULL;
                zframe_destroy(&command);
                //  .split process message
                //  Here is where we actually have a message to process; we
                //  return it to the caller application:
                return msg; //  We have a request to process
            }
            else if (zframe_streq(command, IDPW_HEARTBEAT))
                ; //  Do nothing for heartbeats
            else if (zframe_streq(command, IDPW_DISCONNECT))
                this->startWorker();
            else
            {
                zclock_log("E: invalid input message");
                zmsg_dump(msg);
            }
            zframe_destroy(&command);
            zmsg_destroy(&msg);
        }
        else if (--_liveness == 0)
        {
            if (_verbose)
                zclock_log("W: disconnected from broker - retrying...");
            zclock_sleep(_reconnect_timeout);
            this->startWorker();
        }
        //  Send HEARTBEAT if it's time
        if (zclock_time() > _heartbeat_at)
        {
            send_to_broker(IDPW_HEARTBEAT, NULL, NULL);
            _heartbeat_at = zclock_time() + _heartbeat;
        }
    }
    if (zctx_interrupted)
        printf("W: interrupt received, killing worker...\n");
    zsock_destroy((zsock_t **)&_worker);
    return NULL;
}

void IDP::IDPWorker::loop(void)
{
    zmsg_t *reply = NULL;
    while (true)
    {
        std::vector<std::pair<unsigned char *, size_t>> request_vector;
        std::vector<zframe_t *> request_parts;
        zmsg_t *request = this->receive(&reply);
        if (request == NULL)
            break; //  Worker was interrupted

        zframe_t *part = zmsg_pop(request);
        unsigned char *frame_data = NULL;
        size_t frame_size = 0;
        while (part)
        {
            frame_data = zframe_data(part);
            frame_size = zframe_size(part);
            std::pair<unsigned char *, size_t> p(frame_data, frame_size);
            request_vector.insert(request_vector.begin(), p);
            request_parts.push_back(part);
            part = zmsg_pop(request);
        }
        zmsg_destroy(&request);
        std::vector<std::pair<unsigned char *, size_t>> reply_vector = this->callback(request_vector);
        reply = zmsg_new();
        for (auto it = reply_vector.begin(); it != reply_vector.end(); it++)
        {
            zmsg_pushmem(reply, it->first, it->second);
        }
        for (auto it = request_parts.begin(); it != request_parts.end(); it++)
        {
            zframe_destroy(&(*it));
        }
    }
}
