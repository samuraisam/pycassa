__all__ = ['ConsistencyLevel']

class ConsistencyLevel:
    """
    The ConsistencyLevel is an enum that controls both read and write behavior based on your ReplicationFactor.

    The different consistency levels have different meanings, depending on if you're doing a write or read
    operation. Note that if ``W + R > ReplicationFactor``, where `W` is the number of nodes to block for on write, and `R`
    the number to block for on reads, you will have strongly consistent behavior; that is, readers will always see the most
    recent write.

    Of these, the most interesting is to do `QUORUM` reads and writes, which gives you consistency while still
    allowing availability in the face of node failures up to half of ReplicationFactor. Of course if latency is more
    important than consistency then you can use lower values for either or both.

    """

    ZERO = 0
    """ Ensure nothing for writes.  Not available for reads. Removed in Cassandra 0.7.0. """

    ONE = 1
    """
    For writes, Ensure that the write has been written to at least 1 node's commit log and memory table.

    For reads, this Will return the record returned by the first node to respond. A consistency
    check is always done in a background thread to fix any consistency issues when
    ``ConsistencyLevel.ONE`` is used. This means subsequent calls will have correct data even if the
    initial read gets an older value. (This is called 'read repair'.)
    """

    QUORUM = 2
    """
    For writes, ensure that the write has been written to ``ReplicationFactor/2 + 1`` nodes.

    For reads, this will query all storage nodes and return the record with the most recent
    timestamp once it has at least a majority of replicas reported. Again, the remaining
    replicas will be checked in the background.
    """

    LOCAL_QUORUM = DCQUORUM = 3
    """
    For writes, Ensure that the write has been written to ``ReplicationFactor/2 + 1`` nodes, within the
    local datacenter (requires NetworkTopologyStrategy).

    For reads, returns the record with the most recent timestamp once a majority of replicas within the
    local datacenter have replied.
    """

    DCQUORUM= LOCAL_QUORUM
    """ Alias for :attr:`LOCAL_QUORUM` """

    EACH_QUORUM = 4
    """
    For writes, ensure that the write has been written to ``ReplicationFactor/2 + 1`` nodes in each datacenter (requires NetworkTopologyStrategy).

    For reads, returns the record with the most recent timestamp once a majority of replicas within each datacenter have replied.
    """

    DCQUORUMSYNC= EACH_QUORUM
    """ Alias for :attr:`EACH_QUORUM` """

    ALL = 5
    """
    For writes, ensure that the write is written to ReplicationFactor nodes before responding to the client.

    For reads, queries all storage nodes and returns the record with the most recent timestamp.
    """

    ANY = 6
    """
    For writes, ensure that the write has been written once somewhere, including possibly being hinted in a non-target node.

    Not available for reads.
    """
