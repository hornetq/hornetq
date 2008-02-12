/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;


/**
 * The ConnectorRegistry keeps track of RemotingConfigurations and NIOConnectors.
 * 
 * When a {@link MinaService} is started, it register its {@link RemotingConfiguration}.
 * 
 * When a {@link ClientImpl} is created, it gets its {@link NIOConnector} from the
 * ConnectorRegistry using the {@link RemotingConfiguration} corresponding to the server
 * it wants to connect to. If the ConnectionRegistry contains this RemotingConfigurations, it
 * implies that the Client is in the same VM than the server. In that case, we
 * optimize by returning a {@link INVMConnector} regardless of the transport
 * type defined by the RemotingConfiguration
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface ConnectorRegistry
{

   /**
    * @return <code>true</code> if this RemotingConfiguration has not already been
    *         registered, <code>false</code> else
    */
   boolean register(RemotingConfiguration remotingConfig, PacketDispatcher serverDispatcher);

   /**
    * @return <code>true</code> if this RemotingConfiguration was registered,
    *         <code>false</code> else
    */
   boolean unregister(RemotingConfiguration remotingConfig);

   NIOConnector getConnector(RemotingConfiguration remotingConfig);

   /**
    * Decrement the number of references on the NIOConnector corresponding to
    * the RemotingConfiguration.
    * 
    * If there is only one reference, remove it from the connectors Map and
    * returns it. Otherwise return null.
    * 
    * @param remotingConfig
    *           a RemotingConfiguration
    * @return the NIOConnector if there is no longer any references to it or
    *         <code>null</code>
    * @throws IllegalStateException
    *            if no NIOConnector were created for the given RemotingConfiguration
    */
   NIOConnector removeConnector(RemotingConfiguration remotingConfig);

   RemotingConfiguration[] getRegisteredRemotingConfigurations();

   int getConnectorCount(RemotingConfiguration remotingConfig);
}