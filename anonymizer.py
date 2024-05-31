import config
import random
import crypt

SALT = config.md5_salt


class Anonymizer:
    def check_collision(this_id:str, existing_ids:[]):
        return bool
    
    def jitter_wordcount(x:int):
        if x > 500:
            x += random.randint(-5,5)
        else:
            x += random.randint(-2,2)
        return x

    def hash_post(pid:str):
        hash = crypt.crypt(pid, SALT)
        return hash

    def hash_user(uid:str):
        hash = crypt.crypt(uid, SALT)
        return hash

    def hash_thread(tid:str):
        hash = crypt.crypt(tid, SALT)
        return hash

    def hash_name(name:str):
        hash = crypt.crypt(name, SALT)
        return hash

    def hash_avatar(avid:str):
        pass