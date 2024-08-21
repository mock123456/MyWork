# Copyright (C) 2020 Eric Schorn, NCC Group Plc; Provided under the MIT License

# VRF Demonstration (not constant-time)

import sys
import time

if sys.version_info[0] != 3 or sys.version_info[1] < 7:
    print("Requires Python v3.7+")
    sys.exit()

import secrets
import vrf

# Alice generates a secret and public key pair
secret_key = secrets.token_bytes(nbytes=32)
public_key = vrf.get_public_key(secret_key)

# Alice generates a beta_string commitment to share with Bob
alpha_string = b'I bid $100 for the horse named IntegrityChain'
p_status, pi_string = vrf.ecvrf_prove(secret_key, alpha_string)
b_status, beta_string = vrf.ecvrf_proof_to_hash(pi_string)

#
# Alice initially shares ONLY the beta_string with Bob
#

time_now = time.time()
# Later, Bob validates Alice's subsequently shared public_key, pi_string, and alpha_string
result, beta_string2 = vrf.ecvrf_verify(public_key, pi_string, alpha_string)
if p_status == "VALID" and b_status == "VALID" and result == "VALID" and beta_string == beta_string2:
    print("Commitment verified")
time_later = time.time()
print(time_later-time_now)
