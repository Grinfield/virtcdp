import json
import subprocess
import logging

LOG = logging.getLogger(__name__)


from virtcdp import utils


class QemuDriver:
    """Wrapper for qemu executables"""

    def __init__(self, exportName=None):
        self.exportName = exportName

    def map(self, target):
        extent_map, err = utils.execute("qemu-img", "map",
                                        "--output", "json",
                                        target)
        return json.loads(extent_map)

    def create(self, target_file, file_size, disk_format):
        utils.execute("qemu-img", "create", "-f", disk_format,
                      target_file, file_size)
        return True

    def check(self, target):
        check, _ = utils.execute("qemu-img", "check", "--output", "json", target)
        LOG.debug("==> image check: %s", check)

        return True

    def start_nbd_server(self, targetFile, socketFile):
        p = subprocess.Popen(
            [
                "qemu-nbd",
                "--discard=unmap",
                "--format=qcow2",
                "-x",
                "{self.exportName}",
                "{targetFile}",
                "-k",
                "{socketFile}",
            ],
        )

        return p.pid

    def get_size(self, target):
        img_info, err = utils.execute("qemu-img", "info",
                                      "--output", "json",
                                      target)
        return json.loads(img_info)
