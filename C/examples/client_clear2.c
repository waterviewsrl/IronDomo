//
//  Irondomo Protocol CURVE client example
//  Uses the idcli API to hide all IDP aspects
//  Uses Async API client 
//

//  Lets us build this source without creating a library
#include "idcliapi.h"
#define CERTDIR "cert_store"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    idcli_t *session = idcli_new2 ("tcp://localhost:5000", "EchoClient", verbose);

    idcli_connect_to_broker(session);

    int count;

    for (count = 0; count < 100000; count++) {
        zmsg_t *request = zmsg_new ();
        zmsg_pushstr (request, "Hello world");
        idcli_send2 (session, "echo", &request);
    }
    for (count = 0; count < 100000; count++) {
        zmsg_t *reply = idcli_recv2 (session);
        if (reply)
            zmsg_destroy (&reply);
        else
            break;              //  Interrupted by Ctrl-C
    }

    printf ("%d requests/replies processed\n", count);
    idcli_destroy (&session);
    return 0;
}
