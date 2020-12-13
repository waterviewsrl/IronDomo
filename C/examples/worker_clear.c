//
//  Irondomo Protocol CLEAR worker example
//  Uses the idwrk API to hide all IDP aspects
//

//  Lets us build this source without creating a library
#include "idwrkapi.h"

//Random string for identity
char *rand_string(char *str, size_t size)
{
    unsigned char data[size];
    FILE *fp;
    fp = fopen("/dev/urandom", "r");
    fread(&data, 1, size, fp);
    fclose(fp);
    const char charset[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            unsigned char  key = data[n] % (unsigned char) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));

    char identity[32];
    char random_part[5];
    strcpy(identity, "echo_");
    rand_string(random_part, 5);
    strcat(identity,random_part);

    idwrk_t *session = idwrk_new (
        "tcp://localhost:5000", "echo", identity, verbose);
    
    idwrk_connect_to_broker(session);

    zmsg_t *reply = NULL;
    while (1) {
        zmsg_t *request = idwrk_recv (session, &reply);
        if (request == NULL)
            break;              //  Worker was interrupted
        reply = request;        //  Echo is complex... :-)
    }
    idwrk_destroy (&session);
    return 0;
}
