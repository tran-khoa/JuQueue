import socket


def get_ip_interface(ifname):
    """
    Adapted from dask.distributed.

    Get the local IPv4 address of a network interface.

    KeyError is raised if the interface doesn't exist.
    ValueError is raised if the interface does no have an IPv4 address
    associated with it.
    """
    import psutil

    net_if_addrs = psutil.net_if_addrs()

    if ifname not in net_if_addrs:
        allowed_ifnames = list(net_if_addrs.keys())
        raise ValueError(
            "{!r} is not a valid network interface. "
            "Valid network interfaces are: {}".format(ifname, allowed_ifnames)
        )

    for info in net_if_addrs[ifname]:
        if info.family == socket.AF_INET:
            return info.address
    raise ValueError(f"interface {ifname!r} doesn't have an IPv4 address")
