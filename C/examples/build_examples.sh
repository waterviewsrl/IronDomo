gcc -g -I . -I ../include/  broker.c -lczmq -lzmq -o broker
gcc -g -I . -I ../include/  client_clear.c -lczmq -lzmq -o client_clear
gcc -g -I . -I ../include/  client_curve.c -lczmq -lzmq -o client_curve
gcc -g -I . -I ../include/  worker_clear.c -lczmq -lzmq -o worker_clear
gcc -g -I . -I ../include/  worker_curve.c -lczmq -lzmq -o worker_curve