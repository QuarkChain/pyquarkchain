import miniupnpc

from devp2p import slogging

log = slogging.get_logger('p2p.upnp')

_upnp = None

def _init_upnp():
    global _upnp
    if _upnp:
        return _upnp
    u = miniupnpc.UPnP()
    u.discoverdelay = 200
    try:
        log.debug('Discovering... delay=%ums' % u.discoverdelay)
        ndevices = u.discover()
        log.debug('%u device(s) detected', ndevices)
        _upnp = u
    except Exception as e:
        log.debug('Exception :%s', e)
    finally:
        return _upnp


def add_portmap(port, proto, label=''):
    u = _init_upnp()
    try:
        # select an igd
        u.selectigd()
        # display information about the IGD and the internet connection
        log.debug('local ip address %s:', u.lanaddr)
        externalipaddress = u.externalipaddress()
        log.debug('external ip address %s:', externalipaddress)
        log.debug('%s %s', u.statusinfo(), u.connectiontype())
        log.debug('trying to redirect %s port %u %s => %s port %u %s' %
                  (externalipaddress, port, proto, u.lanaddr, port, proto))
        # find a free port for the redirection
        eport = port
        r = u.getspecificportmapping(eport, proto)
        while r != None and eport < 65536:
            eport = eport + 1
            r = u.getspecificportmapping(eport, proto)
        b = u.addportmapping(eport, proto, u.lanaddr, port, label, '')
        if b:
            log.info('Success. Now waiting for %s request on %s:%u' % (proto, externalipaddress,eport))
            return u
        else:
            log.debug('Failed')
    except Exception as e:
        log.debug('Exception :%s', e)

def remove_portmap(u, port, proto):
    if not u:
        return
    try:
        b = u.deleteportmapping(port, proto)
        if b:
            log.debug('Successfully deleted port mapping')
        else:
            log.debug('Failed to remove port mapping')
    except Exception as e:
        log.debug('Exception :%s', e)

