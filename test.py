import secrets
import vrf
import gzip

# 生成一个32bytes的私钥,再从私钥中获取公钥
secret_key = secrets.token_bytes(32)
public_key = vrf.get_public_key(secret_key)

# alpha_string为要加密的内容,beta为加密之后的hash值,即为生成的随机数
alpha_string = b'I bid $100 for the horse named IntegrityChain'
p_status, pi_string = vrf.ecvrf_prove(secret_key, alpha_string)
b_status, beta_string = vrf.ecvrf_proof_to_hash(pi_string)
print(beta_string)


result, beta_string2 = vrf.ecvrf_verify(public_key, pi_string, alpha_string)
if p_status == "VALID" and b_status == "VALID" and result == "VALID" and beta_string == beta_string2:
    print("Commitment verified")