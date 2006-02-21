/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.tools.jmx.rmi;

import java.rmi.Remote;
import java.util.Set;

import javax.management.ObjectName;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.plugin.contract.DurableSubscriptionStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.remoting.transport.Connector;

/**
 * The remote interface exposed by TestServer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Server extends Remote
{
   void start(String containerConfig) throws Exception;
   void stop() throws Exception;
   void destroy() throws Exception;

   /**
    * Deploys and registers a service based on the MBean service descriptor element, specified as
    * a String. Supports XMBeans. The implementing class and the ObjectName are inferred from the
    * mbean element. If there are configuration attributed specified in the deployment descriptor,
    * they are applied to the service instance.
    */
   ObjectName deploy(String mbeanConfiguration) throws Exception;
   void undeploy(ObjectName on) throws Exception;
   Object getAttribute(ObjectName on, String attribute) throws Exception;
   void setAttribute(ObjectName on, String name, String valueAsString) throws Exception;
   Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception;
   /**
    * Returns a set of ObjectNames corresponding to installed services.
    */
   Set query(ObjectName pattern) throws Exception;

   /**
    * Only for remote use!
    */
   void log(int level, String text) throws Exception;

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   void startServerPeer(String serverPeerID,
                        String defaultQueueJNDIContext,
                        String defaultTopicJNDIContext) throws Exception;
   void stopServerPeer() throws Exception;
   boolean isServerPeerStarted() throws Exception;

   public ObjectName getServerPeerObjectName() throws Exception;
   public ObjectName getDurableSubscriptionStoreObjectName() throws Exception;

   boolean isStarted() throws Exception;

   /**
    * Only for in-VM use!
    */
   Connector getConnector() throws Exception;

   /**
    * Only for in-VM use!
    */
   MessageStore getMessageStore() throws Exception;

   /**
    * Only for in-VM use!
    */
   DestinationManager getDestinationManager() throws Exception;

   /**
    * Only for in-VM use!
    */
   DurableSubscriptionStore getDurableSubscriptionStore() throws Exception;
   
   /**
    * Only for in-VM use
    */
   ServerPeer getServerPeer() throws Exception;

   void deployTopic(String name, String jndiName) throws Exception;
   void deployQueue(String name, String jndiName) throws Exception;
   void undeployDestination(boolean isQueue, String name) throws Exception;

   /**
    * @param config - sending 'config' as a String and not as an org.w3c.dom.Element to avoid
    *        NotSerializableExceptions that show up when running tests on JDK 1.4.
    */
   void configureSecurityForDestination(String destName, String config) throws Exception;

   /**
    * @param config - sending 'config' as a String and not as an org.w3c.dom.Element to avoid
    *        NotSerializableExceptions that show up when running tests on JDK 1.4.
    */
   void setDefaultSecurityConfig(String config) throws Exception;

   /**
    * @return a String that can be converted to an org.w3c.dom.Element using
    *         ServerManagement.toElement().
    */
   String getDefaultSecurityConfig() throws Exception;

   void exit() throws Exception;
   
   /**
    * Executes a command on the server
    * 
    * @param command
    * @return the return value
    * @throws Exception
    */
   Object executeCommand(Command command) throws Exception;

}
