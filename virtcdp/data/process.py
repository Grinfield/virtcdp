import logging
import datetime
import os
import time
import glob
import nbd

from oslo_concurrency import processutils
from oslo_utils import timeutils

from virtcdp.data import extent
from virtcdp.data import frame
from virtcdp.data import extent_driver
from virtcdp.data import nbd_driver
from virtcdp import exception
from virtcdp import utils

LOG = logging.getLogger(__name__)


class ProcessFactory(object):

    def __init__(self):
        self.extent_handler = extent.ExtentHandler(
            extent_driver.QemuDriver())
        self.frame_handler = frame.FrameHandler()
        self.qemu_driver = extent_driver.QemuDriver()

    def post_image_handle(self, disk_name, target,
                          format="qcow2", sync="full"):
        self.check_image(target)
        self.dump_image(disk_name, target, format, sync)

    def dump_image(self, disk_name, target,
                   format="qcow2", sync="full",
                   compressed=False, compressed_method=None):
        if format not in ["qcow2", "raw"]:
            LOG.error("Image format should be qcow2 or raw.")
            raise exception.InvalidInput("Image format must be qcow2 or raw.")

        exts = self.extent_handler.query_extents(target)
        virt_size = self.extent_handler.get_size(target).get("virtual-size")
        thin_size = sum([ext.length for ext in exts if ext.data is True])
        LOG.debug("Target %s got %d extents, virtual size %d(%.2fM),"
                  " thin size %d(%.2fM)",
                  target, len(exts), virt_size, virt_size/utils.UNITS.Mi,
                  thin_size, thin_size/utils.UNITS.Mi)

        no_dirty = False
        if sync == "incremental" and thin_size == 0:
            LOG.info("No dirty blocks found for backup %s.", target)
            no_dirty = True

        client, connection = None, None
        try:
            new_file = target + "@"
            LOG.info("Write data to target file: %s", new_file)

            # read data via qemu-nbd
            if not no_dirty:
                sock_file = self.get_sock_file(action="backup")
                self.start_nbd_server(disk_name, target,
                                      sock_file, self.qemu_driver)
                client, connection = self.connect_nbd_server(disk_name, sock_file)

            with open(new_file, "wb") as writer:
                metadata = {
                    "virtualSize": virt_size,
                    "dataSize": thin_size,
                    "date": datetime.datetime.now().isoformat(),
                    "diskName": disk_name,
                    "compressed": compressed,
                    "compressionMethod": compressed_method,
                    "incremental": sync == "incremental",
                }
                LOG.debug("==> target: %s, metadata: %s", new_file, metadata)

                self._write_header(writer, metadata)
                self._write_extents(writer, client, sync, exts, connection)
                self._write_endian(writer)

            # unlink the original backup file
            os.remove(target)

        except OSError as e:
            LOG.error(f"IO error: {e}")
            raise
        except nbd.Error as e:
            LOG.error(f"NBD handling error: {e.string}({e.errno})")
            raise
        except Exception as e:
            raise e
        finally:
            if client:
                client.disconnect()

        return True

    def check_image(self, target):
        while True:
            try:
                self.qemu_driver.check(target)
            except processutils.ProcessExecutionError as e:
                if e.exit_code == 3:
                    LOG.warning("Checking image %s: image has leaked clusters.", target)
                    time.sleep(0.5)
                    continue
                elif e.exit_code == 1 and \
                        'Failed to get shared \"write\" lock' in e.stderr:
                    LOG.warning("Qemu still hold write lock, wait util it releases.")
                    time.sleep(0.5)
                    continue
                else:
                    LOG.error("Check image %s failed: %s", target, e.exit_code)
                    raise
            else:
                break

    @exception.wrap_exception(reraise=False)
    def load_data(self, uuid, block, format, util_ts, data_dir, restore_dir):
        """Run this function in a co-routine, fulfilling image restoring."""
        with timeutils.StopWatch() as sw:
            self._do_load_data(block, format, util_ts, data_dir, restore_dir)
            LOG.info('Took %0.2f seconds to restore image for %s.',
                     sw.elapsed(), uuid)

    def _do_load_data(self, block, format, util_ts, data_dir, restore_dir):
        # we only support qcow2 temporarily
        format = "qcow2"
        # find the latest full-backup image and inc-backup images
        # followed until the util_ts to make a list, and then sort
        # these images by created timestamp each.
        images = self._get_backup_images(data_dir, util_ts)

        if len(images) < 1:
            LOG.warning("Didn't find any backup for disk image %s.", data_dir)
            return False

        if "FULL" not in images[0]:
            LOG.error("Unable to locate base full backup at %s.", images[0])
            raise exception.NoFullImageException(data_dir=data_dir)

        meta = self.read_image_metadata(images[0])

        target_file = self.create_restore_file(meta, self.qemu_driver,
                                               block,
                                               format if format else block.format,
                                               restore_dir)

        sock_file = self.get_sock_file(action="restore")
        self.start_nbd_server(meta["diskName"], target_file,
                              sock_file, self.qemu_driver)
        client, connection = self.connect_nbd_server(meta["diskName"], sock_file)

        # with open(target_file, "wb") as writer:
        #     for img in images:
        #         self.read2write(img, target_file, writer)
        try:
            for img in images:
                self.read2write(img, target_file, client, connection)
        finally:
            client.disconnect()

    def _write_header(self, writer, metadata):

        self.frame_handler.write_meta(writer, metadata)

    def _write_extents(self, writer, client, sync, exts, connection):

        for ext in exts:
            if ext.data:
                if not connection:
                    LOG.error("Connection should not be None, it it initialized?")
                    raise ValueError("Connection to NBD server is None,"
                                     " while dirty block need be written through it.")
                self.frame_handler.write_data(writer, client, ext, connection)
            # Note: only when full backup, need we write zero frame to image
            if ext.zero and sync == "full":
                self.frame_handler.write_zero(writer, ext)

    def _write_endian(self, writer):
        self.frame_handler.write_stop(writer)

    def create_restore_file(self, meta, qFh, disk, format, dir):
        target_file = os.path.join(dir, disk.node + ".%s" % int(time.time()))

        LOG.info("Create virtual Disk [%s] format: [%s]",
                 target_file, format)
        LOG.info("Virtual Size %s", meta["virtualSize"])

        try:
            qFh.create(target_file, meta["virtualSize"], format)
        except Exception as e:
            LOG.error("Can't create restore image: %s", e)
            raise e

        return target_file

    def create_dump_file(self, target, qFh, format, size):
        target_file = target + "@"
        LOG.info("Create virtual Disk [%s] format: [%s]",
                 target_file,
                 format)
        LOG.info("Virtual Size %s", size)

        try:
            qFh.create(target_file, size, format)
        except Exception as e:
            LOG.error("Can't create dump file: %s", e)
            raise e

        return target_file

    def start_nbd_server(self, disk_name, target_file, sock_file, qFh):
        LOG.info("Starting nbd server on socket: %s", sock_file)

        try:
            nbd_srv = qFh.start_nbd_server(disk_name, target_file, sock_file)
            LOG.info("NBD Server PID: %s", nbd_srv)
        except Exception as e:
            logging.error("Unable to start nbd server: %s", e)
            raise RuntimeError("Unable to start nbd server")

    def connect_nbd_server(self, disk_name, sock_file):
        nbd_client = nbd_driver.NBDClient(disk_name, None, sock_file)
        LOG.info("Waiting until nbd server on socket %s is up.", sock_file)
        retry = 0
        max_retry = 20
        while True:
            if os.path.exists(sock_file):
                connection = nbd_client.connect()
                if connection:
                    LOG.info("Connection to nbd backend succeeded.")
                    break
            else:
                if retry >= max_retry:
                    LOG.error("NBD server connection failed.")
                    raise RuntimeError("NBD server connection failed.")

                LOG.info("Waiting for NBD Server, Retry: %s", retry)
                time.sleep(1)
                retry += 1

        return nbd_client, connection

    @staticmethod
    def get_sock_file(sock_path=None, action="backup"):
        if sock_path is None:
            sock_file = f"/var/tmp/virtcdp.{action}.{os.getpid()}"
        else:
            sock_file = sock_path

        return sock_file

    def read_image_metadata(self, image):
        """read metadata header"""
        with open(image, "rb") as reader:
            try:
                meta = self.frame_handler.read_meta(reader)
            except Exception as e:
                LOG.error("Reading metadata from %s failed: %s", image, e)
                raise
        return meta

    def read2write(self, data_file, target_file, client, connection):
        """Restore data for disk"""
        reader = None
        try:
            reader = open(data_file, "rb")
            # read metadata frame
            meta = self.frame_handler.read_meta(reader)

            if meta["dataSize"] == 0:
                LOG.info("Saveset %s contains no dirty blocks, skipping.", data_file)
                return True

            LOG.info("Applying data from backup file [%s] to target file [%s]",
                     data_file, target_file)
            self.frame_handler.test(reader)

            # Read data frame behind metadata frame util stop frame
            self.frame_handler.read_all(meta, reader, client, connection)
        except OSError as e:
            LOG.error("Error occurred in reading image %s: %s.", data_file, e)
            raise
        except nbd.Error as e:
            LOG.error(f"NBD handling error occurred: {e.string}({e.errno})")
            raise
        except Exception as e:
            LOG.error("Error occurred in restoring image: %s", e)
            raise
        finally:
            if reader:
                reader.close()

    def _get_backup_images(self, data_dir, util):
        files = iter(glob.glob(os.path.join(data_dir, "*@")))

        tgt_imgs = []
        full_image = None

        for f in sorted(files, key=os.path.getctime, reverse=True):
            spls = os.path.basename(f).split("-")

            if spls[0] not in ("FULL", "INC") or int(spls[1][:-1]) > util:
                continue

            tgt_imgs.append(f)
            # if we first touch a FULL image, then images we need are enough
            if spls[0] == "FULL":
                full_image = f
                break

        if full_image is None:
            raise RuntimeError("No 'FULL' backup images are found,"
                               " we can't execute restoring action.")
        # Reverse the list to make it sort by its created time
        tgt_imgs.reverse()

        return tgt_imgs
