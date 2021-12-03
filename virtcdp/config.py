import socket

from oslo_config import cfg

CONF = cfg.CONF

global_opts = [
    cfg.BoolOpt('debug',
                default=False,
                help="Turn on debug log."),
    cfg.StrOpt("host",
               default=socket.gethostname(),
               help="Hostname, Default is hostname of this host."),
    cfg.HostAddressOpt('my_ip',
                       default='127.0.0.1',
                       help="My host ip."),
    cfg.StrOpt('virtcdp_server_listen',
               default="0.0.0.0",
               help="IP address on which the virtcdp server API will listen."),
    cfg.PortOpt('virtcdp_server_listen_port',
                default=8663,
                help="Port on which the virtcdp server API will listen."),
    cfg.IntOpt('virtcdp_server_workers',
               min=1,
               help="Number of workers for virtcdp server API service."
                    "The default will be the number of CPUs available."),
    cfg.StrOpt('log_file',
               default='/var/log/virtcdp/%s.log',
               help="Log file path."),
]

rpc_opts = [
    cfg.StrOpt('rpc_server_listen',
               default="127.0.0.1",
               help="IP address on which the RPC server will listen."),
    cfg.PortOpt('rpc_server_listen_port',
                default=8000,
                help="Port on which the RPC server will listen."),

]

libvirt_group = cfg.OptGroup("libvirt",
                             title="Libvirt Options",
                             help="""
Libvirt options allows cloud administrator to configure related
libvirt hypervisor driver to be used within an OpenStack deployment.

Almost all of the libvirt config options are influence by ``virt_type`` config
which describes the virtualization type (or so called domain type) libvirt
should use for specific features such as live migration, snapshot.
""")

libvirt_opts = [
    cfg.StrOpt('virt_type',
               default='kvm',
               choices=('kvm', 'lxc', 'qemu', 'uml', 'xen', 'parallels'),
               help="""
Describes the virtualization type (or so called domain type) libvirt should
use.

The choice of this type must match the underlying virtualization strategy
you have chosen for this host.

Possible values:

* See the predefined set of case-sensitive values.

Related options:

* ``connection_uri``: depends on this
* ``disk_prefix``: depends on this
* ``cpu_mode``: depends on this
* ``cpu_model``: depends on this
"""),
    cfg.BoolOpt(
        'handle_virt_lifecycle_events',
        default=True,
        help="""
Enable handling of events emitted from compute drivers."""),
    cfg.StrOpt('connection_uri',
               default='',
               help="""
Overrides the default libvirt URI of the chosen virtualization type.

If set, virtcdp will use this URI to connect to libvirt.

Possible values:

* An URI like ``qemu:///system`` or ``xen+ssh://oirase/`` for example.
  This is only necessary if the URI differs to the commonly known URIs
  for the chosen virtualization type.

Related options:

* ``virt_type``: Influences what is used as default value here.
"""),
]


CONF.register_opts(global_opts)
CONF.register_opts(rpc_opts)

CONF.register_group(libvirt_group)
CONF.register_opts(libvirt_opts, group=libvirt_group)


def parse_args(argv, default_config_files=None):
    CONF(argv[1:],
         project='virtcdp',
         default_config_files=default_config_files)
