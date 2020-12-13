# IronDomo
ZeroMQ Majordomo Pattern with CURVE authentication, allowing encripted and clear communication between clients and workers.

Extends ZMQ Majordomo (https://rfc.zeromq.org/spec:7/MDP) pattern with CURVE authentication and encription  
It provides a Broker with two endpoints for client connection, providing both a clear and a trusted communication channels. 

![Irondomo Sketch](https://rfc.zeromq.org/rfcs/7/1.png)

## APIs
APIs and examples are provided for python3 and C/C++.

### Python 

The package can be installed from source or directly from pip

```shell
pip install irondomo
```
Examples provide documentation of the various APIs and necessary configuration

### C/C++

APIs are provided for C and C++ as header only distribution in order to simplify target build. 
Examples provide documentation of the various APIs and necessary configuration


