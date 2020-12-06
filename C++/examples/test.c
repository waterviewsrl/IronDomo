// Create temporary directory for test files
# define TESTDIR ".test_zauth"
zsys_dir_create (TESTDIR);

// Check there's no authentication
zsock_t *server = zsock_new (ZMQ_PUSH);
assert (server);
zsock_t *client = zsock_new (ZMQ_PULL);
assert (client);
bool success = s_can_connect (&server, &client);
assert (success);

// Install the authenticator
zactor_t *auth = zactor_new (zauth, NULL);
assert (auth);
if (verbose) {
 zstr_sendx (auth, "VERBOSE", NULL);
 zsock_wait (auth);
}
// Check there's no authentication on a default NULL server
success = s_can_connect (&server, &client);
assert (success);

// When we set a domain on the server, we switch on authentication
// for NULL sockets, but with no policies, the client connection
// will be allowed.
zsock_set_zap_domain (server, "global");
success = s_can_connect (&server, &client);
assert (success);

// Blacklist 127.0.0.1, connection should fail
zsock_set_zap_domain (server, "global");
zstr_sendx (auth, "DENY", "127.0.0.1", NULL);
zsock_wait (auth);
success = s_can_connect (&server, &client);
assert (!success);

// Whitelist our address, which overrides the blacklist
zsock_set_zap_domain (server, "global");
zstr_sendx (auth, "ALLOW", "127.0.0.1", NULL);
zsock_wait (auth);
success = s_can_connect (&server, &client);
assert (success);

// Try PLAIN authentication
zsock_set_plain_server (server, 1);
zsock_set_plain_username (client, "admin");
zsock_set_plain_password (client, "Password");
success = s_can_connect (&server, &client);
assert (!success);

FILE *password = fopen (TESTDIR "/password-file", "w");
assert (password);
fprintf (password, "admin=Password\n");
fclose (password);
zsock_set_plain_server (server, 1);
zsock_set_plain_username (client, "admin");
zsock_set_plain_password (client, "Password");
zstr_sendx (auth, "PLAIN", TESTDIR "/password-file", NULL);
zsock_wait (auth);
success = s_can_connect (&server, &client);
assert (success);

zsock_set_plain_server (server, 1);
zsock_set_plain_username (client, "admin");
zsock_set_plain_password (client, "Bogus");
success = s_can_connect (&server, &client);
assert (!success);

if (zsys_has_curve ()) {
 // Try CURVE authentication
 // We'll create two new certificates and save the client public
 // certificate on disk; in a real case we'd transfer this securely
 // from the client machine to the server machine.
 zcert_t *server_cert = zcert_new ();
 assert (server_cert);
 zcert_t *client_cert = zcert_new ();
 assert (client_cert);
 char *server_key = zcert_public_txt (server_cert);

 // Test without setting-up any authentication
 zcert_apply (server_cert, server);
 zcert_apply (client_cert, client);
 zsock_set_curve_server (server, 1);
 zsock_set_curve_serverkey (client, server_key);
 success = s_can_connect (&server, &client);
 assert (!success);

 // Test CURVE_ALLOW_ANY
 zcert_apply (server_cert, server);
 zcert_apply (client_cert, client);
 zsock_set_curve_server (server, 1);
 zsock_set_curve_serverkey (client, server_key);
 zstr_sendx (auth, "CURVE", CURVE_ALLOW_ANY, NULL);
 zsock_wait (auth);
 success = s_can_connect (&server, &client);
 assert (success);

 // Test full client authentication using certificates
 zcert_apply (server_cert, server);
 zcert_apply (client_cert, client);
 zsock_set_curve_server (server, 1);
 zsock_set_curve_serverkey (client, server_key);
 zcert_save_public (client_cert, TESTDIR "/mycert.txt");
 zstr_sendx (auth, "CURVE", TESTDIR, NULL);
 zsock_wait (auth);
 success = s_can_connect (&server, &client);
 assert (success);

 zcert_destroy (&server_cert);
 zcert_destroy (&client_cert);
}
// Remove the authenticator and check a normal connection works
zactor_destroy (&auth);
success = s_can_connect (&server, &client);
assert (success);

zsock_destroy (&client);
zsock_destroy (&server);

// Delete all test files
zdir_t *dir = zdir_new (TESTDIR, NULL);
assert (dir);
zdir_remove (dir, true); zdir_destroy (&dir);
