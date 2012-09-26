package org.hornetq.core.cluster;

import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.api.core.UDPBroadcastEndpoint;
import org.hornetq.utils.ClassloadingUtil;

public interface BroadcastEndpointFactory
{
    BroadcastEndpoint createBroadcastEndpoint() throws Exception;
}
