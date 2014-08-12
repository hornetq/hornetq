package org.hornetq.api.core;

import java.io.Serializable;


//XXX No javadocs
public interface BroadcastEndpointFactory extends Serializable
{
   BroadcastEndpoint createBroadcastEndpoint() throws Exception;
}
