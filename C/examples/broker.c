#include "../include/idbrokerapi.h" 
/*
Example of broker with no cert store configured
*/

int main(int argc, char *argv[])
{
    int _verbose = (argc > 1 && streq(argv[1], "-v"));

    //zcert_t * c = zcert_new();
    //zclock_log("I: secret_key: %s", zcert_secret_txt(c));
    //zclock_log("I: public_key: %s", zcert_public_txt(c));

    const char public_key[] = ".8Q^k*3E/4-Wg4()r^(4yTk2>qvZFDW?mXUyRPvr";
    const char secret_key[] = "3vup%:I!lF>^QWT@[[g]dwa>1:(B-^3RWw^7tIMf";
    

    broker_t *self = s_broker_new("tcp://127.0.0.1:5000", "tcp://127.0.0.1:5001", public_key, secret_key, NULL, _verbose);
    
    s_broker_loop(self);
  
    if (zctx_interrupted)
        printf("W: interrupt received, shutting down...\n");

    s_broker_destroy(&self);
    return 0;
}
