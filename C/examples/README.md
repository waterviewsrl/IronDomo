# C Examples

## Build 

In order to build the C examples, it is necessary to install on the machin zmq and czmq libraries.

Examples are built with the script build_examples.sh:

```shell
bash build_examples.sh
```

## List of Examples:

* broker.c: a broker with clear socket configured on port 5000 and CURVE socket configured on port 5001. Workers and client on the CURVE socket connect are encripted, but client public key is not authenticated looking up a certificate store 
* broker_certstore.c: a broker with clear socket configured on port 5000 and CURVE socket configured on port 5001. Workers and client on the CURVE socket connect are encripted, and authenticated looking up a certificate store. In this case public keys need to be stored in a directory (cert_store in the example) for the broker to be able to perform the authentication
* client_clear.c: unencrypted client
* client_clear2.c: unencrypted client with async interface
* client_curve.c: encrypted client
* client_curve2.c: encrypted client with async interface
* worker_clear.c: unencrypted worker
* worker_curve.c: encrypted worker
