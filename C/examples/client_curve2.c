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
    idcli_t *session = idcli_new2 ("tcp://localhost:5001", "EchoClient", verbose);

    zcert_t *c = zcert_new();
    zcert_save_public(c, CERTDIR"/client_cert.txt");

    const char *client_public = zcert_public_txt(c);
    const char *client_secret = zcert_secret_txt(c);
    const char *server_public = ".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr";

    idcli_setup_curve(session, client_public, client_secret, server_public);

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
    zcert_destroy(&c);
    return 0;
}
