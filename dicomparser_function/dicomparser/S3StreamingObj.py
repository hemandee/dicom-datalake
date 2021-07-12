import io
from io import UnsupportedOperation

class S3StreamObj(io.RawIOBase):
# https://alexwlchan.net/2019/02/working-with-large-s3-objects/
    def __init__(self, s3file,prefix=""):
        self.s3_object = s3file
        self.position = 0
        self.size = self.s3_object.content_length
        self.totalbytes = 0
        self.eventGets = 0
        self.prefix = prefix

    def tell(self):
    # https://docs.python.org/3/library/io.html#io.IOBase.seek
        return self.position
    def seekable(self):
        arg = self
        return True
    def seek(self, offset, whence=io.SEEK_SET):
    # https://python-reference.readthedocs.io/en/latest/docs/file/seek.html
    # https://docs.python.org/3/library/io.html#io.IOBase.seek
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise UnsupportedOperation("UnSupported Operation (%r) %s" % (whence, "https://docs.python.org/3.8/library/os.html#os.SEEK_SET"))

        return self.position


    def setBytesHeader(self,start,end=''):
        return "bytes={}-{}".format(start,end)

    def read(self, size=-1):
    # https://docs.python.org/3/library/io.html#io.IOBase.seek    
        if size == -1:
            bytesHeader = self.setBytesHeader(self.position)
            self.totalbytes += (self.size - self.position)
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            # range_header = "bytes=%d-%d" % (self.position, new_position - 1)
            # minus 1 since byte positions are inclusive 
            bytesHeader = self.setBytesHeader(self.position,new_position - 1)
            # self.totalbytes += (new_position - 1 - self.position)
            self.seek(offset=size, whence=io.SEEK_CUR)
        self.eventGets += 1
        event = self.s3_object.get(Range=bytesHeader)
        self.totalbytes += event["ContentLength"]
        return event["Body"].read()
    
    def readinto(self,b):
    # https://github.com/python/cpython/blob/6fdfcec5b11f44f27aae3d53ddeb004150ae1f61/Modules/_io/bytesio.c#L564    
    # Ignore WRITE as only READ required
            return ""


    def readall(self):
        return self.s3_object.get()["Body"].read()
    def readinto(b):
    # https://docs.python.org/3/library/io.html#io.RawIOBase
    # Ignoring WRITE as only READ required
        return ""    
    def write():
    # https://docs.python.org/3/library/io.html#io.RawIOBase    
    # Ignoring WRITE as only READ required    
        return ""