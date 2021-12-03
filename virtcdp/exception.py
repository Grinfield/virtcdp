import logging

LOG = logging.getLogger(__name__)


class VirtcdpException(Exception):
    """Base Virtcdp Exception

    To correctly use this class, inherit from it and define
    a 'msg_fmt' property. That msg_fmt will get printf'd
    with the keyword arguments provided to the constructor.

    """
    msg_fmt = "An unknown exception occurred."
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        if not message:
            try:
                message = self.msg_fmt % kwargs

            except Exception:
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception('Exception in string format operation')
                for name, value in kwargs.items():
                    LOG.error("%s: %s" % (name, value))  # noqa

                message = self.msg_fmt

        self.message = message
        super(VirtcdpException, self).__init__(message)

    def format_message(self):
        # NOTE(mrodden): use the first argument to the python Exception object
        # which should be our full NovaException message, (see __init__)
        return self.args[0]


class HypervisorUnavailable(VirtcdpException):
    msg_fmt = "Connection to the hypervisor is broken on host: %(host)s"


class InternalError(VirtcdpException):
    """Generic hypervisor errors.

    Consider subclassing this to provide more specific exceptions.
    """
    msg_fmt = "%(err)s"


class NotFound(VirtcdpException):
    msg_fmt = "Resource could not be found."
    code = 404


class InstanceNotFound(NotFound):
    msg_fmt = "Instance %(instance_id)s could not be found."


class NoBlockdevsFound(NotFound):
    msg_fmt = "Instance %(instance_id)s has no any block devices."


class QemuMonitorCommandError(VirtcdpException):
    msg_fmt = "Failed to execute qemu monitor command '%(cmd)s' " \
              "for domain '%(domain)s', arguments: '%(args)s', class: " \
              "'%(cls)s', desc: '%(desc)s'."


class Invalid(VirtcdpException):
    msg_fmt = "Bad Request - Invalid Parameters"
    code = 400


class InvalidInput(Invalid):
    msg_fmt = "Invalid input received: %(reason)s"


class IncBackupNoBitmapException(Invalid):
    msg_fmt = "Device %(dev)s of instance %(uuid)s do incremental " \
              "backup without bitmap."


class InstanceNotInBackup(Invalid):
    msg_fmt = "Requested instance %(uuid)s has no backup task."


class DiskNotInBackup(Invalid):
    msg_fmt = "Requested disk %(disk)s of instance %(uuid)s has no backup task."


class NoFullImageException(Invalid):
    msg_fmt = "No full backup image found at the directory %(data_dir)s."
