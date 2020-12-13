#include "../include/idbrokerapi.h" 
/*
Example of broker with cert store configured on directory cert_store where the public keys of curve 
enabled clients and workers are stored
*/

#define CERTDIR "cert_store"

int main(int argc, char *argv[])
{
    int _verbose = (argc > 1 && streq(argv[1], "-v"));

    /*
    Uncomment to generate new keys at each execution. Public key must be provided to curve enabled
    clients and workers
    */
    //zcert_t * c = zcert_new();
    //zclock_log("I: secret_key: %s", zcert_secret_txt(c));
    //zclock_log("I: public_key: %s", zcert_public_txt(c));

    const char public_key[] = ".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr";
    const char secret_key[] = "3vup%:I!lF>^QWT@[[g]dwa>1:(B-^3RWw^7tIMf";
    

    broker_t *self = s_broker_new("tcp://127.0.0.1:5000", "tcp://127.0.0.1:5001", public_key, secret_key, CERTDIR, _verbose);
    
    s_broker_loop(self);
  
    if (zctx_interrupted)
        printf("W: interrupt received, shutting down...\n");

    s_broker_destroy(&self);
    return 0;
}
