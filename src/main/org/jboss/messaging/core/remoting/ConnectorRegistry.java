/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.ClientImpl;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;


/**
 * The ConnectorRegistry keeps track of ServerLocators and NIOConnectors.
 * 
 * When a {@link MinaService} is started, it register its {@link ServerLocator}.
 * 
 * When a {@link ClientImpl} is created, it gets its {@link NIOConnector} from the
 * ConnectorRegistry using the {@link ServerLocator} corresponding to the server
 * it wants to connect to. If the ConnectionRegistry contains this locator, it
 * implies that the Client is in the same VM than the server. In that case, we
 * optimize by returning a {@link INVMConnector} regardless of the transport
 * type defined by the locator
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface ConnectorRegistry
{

   /**
    * @return <code>true</code> if this locator has not already been
    *         registered, <code>false</code> else
    */
   boolean register(ServerLocator locator);

   /**
    * @return <code>true</code> if this locator was registered,
    *         <code>false</code> else
    */
   boolean unregister(ServerLocator locator);

   NIOConnector getConnector(ServerLocator locator);

   /**
    * Decrement the number of references on the NIOConnector corresponding to
    * the locator.
    * 
    * If there is only one reference, remove it from the connectors Map and
    * returns it. Otherwise return null.
    * 
    * @param locator
    *           a ServerLocator
    * @return the NIOConnector if there is no longer any references to it or
    *         <code>null</code>
    * @throws IllegalStateException
    *            if no NIOConnector were created for the given locator
    */
   NIOConnector removeConnector(ServerLocator locator);

   ServerLocator[] getRegisteredLocators();

   int getConnectorCount(ServerLocator locator);

}