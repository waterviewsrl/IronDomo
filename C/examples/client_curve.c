//
//  Irondomo Protocol CURVE client example
//  Uses the idcli API to hide all MDP aspects
//

//  Lets us build this source without creating a library
#include "idcliapi.h"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    idcli_t *session = idcli_new ("tcp://localhost:5001", "EchoClient", verbose);

    zcert_t *c = zcert_new();

    const char *client_public = zcert_public_txt(c);
    const char *client_secret = zcert_secret_txt(c);
    const char *server_public = ".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr";

    idcli_setup_curve(session, client_public, client_secret, server_public);

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
