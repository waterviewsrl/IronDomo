//
//  Irondomo Protocol CLEAR worker example
//  Uses the idwrk API to hide all MDP aspects
//

//  Lets us build this source without creating a library
#include "idwrkapi.h"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    idwrk_t *session = idwrk_new (
        "tcp://localhost:5000", "echo", verbose);
    
    idwrk_connect_to_broker(session);

    zmsg_t *reply = NULL;
    while (1) {
        zmsg_t *request = idwrk_recv (session, &reply);
        if (request == NULL)
            break;              //  Worker was interrupted
	printf("RICEVO\n");
        reply = request;        //  Echo is complex... :-)
    }
    idwrk_destroy (&session);
    return 0;
}
