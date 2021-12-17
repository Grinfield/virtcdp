
import os
import random
import string
import sys
import time


def rand_write(times):
    pwd = os.getcwd()
    newdir = os.path.join(pwd, 'new')
    if not os.path.exists(newdir):
        os.makedirs(newdir)
    gentor = gen_data()
    while True:
        rand_f = os.path.join(newdir, rand_file())
        with open(rand_f, "wb") as new_file:
            lines = next(gentor)
            for i in range(times):
                new_file.writelines(lines)
            new_file.flush()
        time.sleep(1)


def gen_data():
    source = None
    try:
        source = open('/usr/share/dict/words', 'rb')
        while True:
            lines = source.readlines(10000)
            # print(source.tell())
            if not lines:
                source.seek(0, os.SEEK_SET)
                lines = source.readlines(10000)
            yield lines
    except OSError as e:
        print("IO error: %s", e)
        raise
    finally:
        if source:
            source.close()


def rand_file():
    salt = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    return salt


def main(times):
    rand_write(times)
    return 0


if __name__ == "__main__":
    argv = sys.argv
    if len(argv) < 2:
        times = 3
    else:
        times = int(argv[1])
    sys.exit(main(times))
