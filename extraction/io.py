import hashlib
import os


def read_in_chunks(file_object, chunk_size=1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def hash_bytestr_iter(bytesiter, hasher):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest()


def file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)


def sha256(path):
    block_iter = file_as_blockiter(open(path, "rb"))
    hasher = hashlib.sha256()
    return hash_bytestr_iter(block_iter, hasher)


class File(object):
    def __init__(self, path):
        if path and not os.path.exists(path):
            touch(path)
        self.path = path

    def open(self, mode="r"):
        return open(self.path, mode)

    def __len__(self):
        return os.stat(self.path).st_size

    def __iadd__(self, other):
        with open(self.path, "ab") as us:
            with open(other.path, "rb") as them:
                for chunk in read_in_chunks(them, 2 ** 16):
                    us.write(chunk)
        return self

    def __eq__(self, other):
        if self is other:
            return True
        return sha256(self.path) == sha256(other.path)
