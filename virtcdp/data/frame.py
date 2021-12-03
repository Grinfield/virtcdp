import logging
import json

LOG = logging.getLogger(__name__)


class SparseStreamTypes:
    """Sparse stream format

    META:   start of meta information header
    DATA:   data block marker
    ZERO:   zero block marker
    STOP:   stop block marker
    TERM:   termination identifier
    FRAME:  assembled frame
    FRAME_LEN: length of frame

    Stream format
    =============

    Stream is composed of one or more frames.

    Meta frame
    ----------
    Stream metadata, must be the first frame.

    "meta" space start length "\r\n" <json-payload> \r\n

    Metadata keys in the json payload:

    - virtual-size: image virtual size in bytes
    - data-size: number of bytes in data frames
    - date: ISO 8601 date string

    Data frame
    ----------
    The header is followed by length bytes and terminator.
    "data" space start length "\r\n" <length bytes> "\r\n"

    Zero frame
    ----------
    A zero extent, no payload.
    "zero" space start length "\r\n"

    Stop frame
    ----------
    Marks the end of the stream, no payload.
    "stop" space start length "\r\n"

    Regular stream Example
    -------
    meta 0000000000000000 0000000000000083\r\n
    {
        [.]]
    }\r\n
    data 0000000000000000 00000000000100000\r\n
    <1 MiB bytes>\r\n
    zero 0000000000100000 00000000040000000\r\n
    data 0000000040100000 00000000000001000\r\n
    <4096 bytes>\r\n
    stop 0000000000000000 00000000000000000\r\n


    Compressed stream:
    -------
    Ends with compression marker:
    stop 0000000000000000 00000000000000000\r\n
    <json payload with compressed block sizes>\r\n
    comp 0000000000000000 00000000000000010\r\n
    """

    def __init__(self):
        self.META = b"meta"
        self.DATA = b"data"
        self.COMP = b"comp"
        self.ZERO = b"zero"
        self.STOP = b"stop"
        self.TERM = b"\r\n"
        self.FRAME = b"%s %016x %016x" + self.TERM
        self.FRAME_LEN = len(self.FRAME % (self.STOP, 0, 0))


class FrameHandler(object):

    def __init__(self):
        self.types = SparseStreamTypes()

    def _write_frame(self, writer, kind, start, length):
        """Write backup frame
        Parameters:
            writer: (fh)    Writer object that implements .write()
        """
        writer.write(self.types.FRAME % (kind, start, length))

    def _read_frame(self, reader):
        """Read backup frame
        Parameters:
            reader: (fh)    Reader object which implements .read()
        """
        header = reader.read(self.types.FRAME_LEN)
        kind, start, length = header.split(b" ", 2)
        return kind, int(start, 16), int(length, 16)

    def write_meta(self, writer, metadata):
        if isinstance(metadata, dict):
            metadata = json.dumps(metadata, indent=4).encode("utf-8")

        self._write_frame(writer, self.types.META, 0, len(metadata))
        writer.write(metadata)
        writer.write(self.types.TERM)

    def write_data(self, writer, reader, ext):
        self._write_frame(writer, self.types.DATA, ext.offset, ext.length)

        LOG.debug("Read data from: start %s, length: %s",
                  ext.offset, ext.length)

        rest = ext.length
        read = 0
        while rest != 0:
            pos = ext.length + read
            reader.seek(pos)
            data = reader.read(rest)
            LOG.debug("==>[backup] read data: %d", len(data))

            rest -= len(data)
            read += len(data)

            writer.write(data)

        assert read == ext.length
        writer.write(self.types.TERM)

    def write_zero(self, writer, ext):
        self._write_frame(writer, self.types.ZERO, ext.offset, ext.length)

    def write_stop(self, writer):
        self._write_frame(writer, self.types.STOP, 0, 0)

    def read_meta(self, reader):
        _, _, length = self._read_frame(reader)
        content = reader.read(length)
        return json.loads(content.decode("utf-8"))

    def read_data(self):
        pass

    def read_zero(self):
        pass

    def read_stop(self):
        pass

    def test(self, reader):
        # import binascii
        # term = reader.read(len(self.types.TERM))
        # LOG.debug("==> read term: %s", binascii.b2a_hex(term))
        assert reader.read(len(self.types.TERM)) == self.types.TERM

    def read_all(self, meta, reader, writer):
        dataSize = 0
        while True:
            try:
                kind, start, length = self._read_frame(reader)
                LOG.debug("==> kind:%s, start:%016X, length:%016X", kind, start, length)
            except Exception as e:
                LOG.exception("Wrong stream at pos: %s", reader.tell())
                raise

            if kind == self.types.ZERO:
                LOG.debug("Write zero segment from %s length: %s", start, length)
                # connection.zero(length, start)
                # writer.seek(start)
                # writer.write("".zfill(length))

            elif kind == self.types.DATA:
                LOG.debug("Process data segment from %s length: %s", start, length)
                # LOG.debug("==> Read at addr: %s", reader.tell())
                originalSize = length

                try:
                    # connection.pwrite(data, start)
                    writer.seek(start)

                    rest = length
                    read = 0
                    while rest != 0:
                        # pos = length + read
                        # reader.seek(pos)
                        data = reader.read(rest)
                        LOG.debug("==>[restore] read data: %d", len(data))

                        rest -= len(data)
                        read += len(data)

                        writer.write(data)
                except Exception as e:
                    LOG.exception(e)
                    return False

                assert read == length
                self.test(reader)
                dataSize += originalSize

            elif kind == self.types.STOP:
                if dataSize == meta["dataSize"]:
                    LOG.info("End of stream, %s bytes of data processed.", dataSize)
                    return True

                LOG.error("Error: restored data size %s != %s",
                          dataSize,
                          meta["dataSize"])
                return False
