//
//  Irondomo Protocol  CLEAR client example
//  Uses the idcli API to hide all IDP aspects
//

//  Lets us build this source without creating a library
#include "idcliapi.h"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    idcli_t *session = idcli_new ("tcp://localhost:5000", "EchoClient", verbose);

    idcli_connect_to_broker(session);

    int count;
    for (count = 0; count < 2000; count++) {
        zmsg_t *request = zmsg_new ();
        zmsg_pushstr (request, "Hello");
        zmsg_pushstr (request, " ");
        zmsg_pushstr (request, "world");
        zmsg_t *reply = idcli_send (session, "echo", &request);
        if (reply)
            zmsg_destroy (&reply);
        else
            break;              //  Interrupt or failure
    }
    printf ("%d requests/replies processed\n", count);
    idcli_destroy (&session);
    return 0;
}
