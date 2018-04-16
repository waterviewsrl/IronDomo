"""Irondomo Protocol definitions"""
#  This is the version of IDP/Client we implement
C_CLIENT = b"IDPC01"

#  This is the version of IDP/Worker we implement
W_WORKER = b"IDPW01"

#  IDP/Server commands, as strings
W_READY         =   b"\001"
W_REQUEST       =   b"\002"
W_REPLY         =   b"\003"
W_HEARTBEAT     =   b"\004"
W_DISCONNECT    =   b"\005"
W_REQUEST_CURVE =   b"\006"
W_REPLY_CURVE   =   b"\007"

commands = [None, b"READY", b"REQUEST", b"REPLY", b"HEARTBEAT", b"DISCONNECT"]
