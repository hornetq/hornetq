package org.hornetq.api.core;

import org.hornetq.api.core.BroadcastEndpoint;

public interface BroadcastEndpointFactory
{
    BroadcastEndpoint createBroadcastEndpoint() throws Exception;
}
