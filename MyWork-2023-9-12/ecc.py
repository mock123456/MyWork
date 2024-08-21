import nacl.signing
from nacl.signing import SigningKey
from nacl.signing import VerifyKey
import hashlib


# 这个模块主要引用了pynacl包,对一些信息进行数字签名
def generate_sign(chk_msg):
    """对message进行签名,返回签名后的信息和公钥，中间以split二进制数隔开,注意:在网络编程中,一切数据都是byte类型的"""
    # 生成签名的公私钥对
    signing_key = SigningKey.generate()
    # 将消息转换为字符串编码,然后签名
    signed_prepare = signing_key.sign(str(chk_msg).encode())
    verify_key = signing_key.verify_key
    public_key = verify_key.encode()
    # 返回签名内容和公钥,中间以二进制数split分隔开
    return signed_prepare + b'split' + public_key


def generate_verfiy(public_key, rece_msg):
    """对签名进行校验"""
    verify_key = VerifyKey(public_key)
    return verify_key.verify(rece_msg).decode()



def hashing_function(entity):
    """对XXX取哈希值,返回十六进制数,这里存在问题:"""
    h = hashlib.sha256
    h.update(str(entity).encode())
    return h.hexdisgest()


def define_type_of_node():
    pass