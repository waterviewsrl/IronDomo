//
//  Irondomo Protocol CURVE worker example
//  Uses the idwrk API to hide all MDP aspects
//

//  Lets us build this source without creating a library
#include "idwrkapi.h"

int main(int argc, char *argv[])
{
    int verbose = (argc > 1 && streq(argv[1], "-v"));
    idwrk_t *session = idwrk_new("tcp://localhost:5001", "echo", verbose);

    zcert_t *c = zcert_new();

    const char *worker_public = zcert_public_txt(c);
    const char *worker_secret = zcert_secret_txt(c);
    const char *server_public = ".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr";

    idwrk_setup_curve(session, worker_public, worker_secret, server_public);

    idwrk_connect_to_broker(session);

    zmsg_t *reply = NULL;
    while (1)
    {
        zmsg_t *request = idwrk_recv(session, &reply);
        if (request == NULL)
            break; //  Worker was interrupted
        
        reply = request; //  Echo is complex... :-)
    }
    idwrk_destroy(&session);
    return 0;
}
