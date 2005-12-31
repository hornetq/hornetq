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

import org.jboss.messaging.core.MessageStore;
import org.jboss.jms.server.StateManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.remoting.transport.Connector;

import java.rmi.Remote;

/**
 * The remote interface exposed by RMIServer.
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

   void startServerPeer() throws Exception;
   void stopServerPeer() throws Exception;

   boolean isStarted() throws Exception;

   /**
    * Only for in-VM use!
    */
   ServerPeer getServerPeer() throws Exception;

   /**
    * Only for in-VM use!
    */
   Connector getConnector() throws Exception;

   MessageStore getMessageStore() throws Exception;
   StateManager getStateManager() throws Exception;

   void deployTopic(String name, String jndiName) throws Exception;
   void undeployTopic(String name) throws Exception;
   void deployQueue(String name, String jndiName) throws Exception;
   void undeployQueue(String name) throws Exception;


   /**
    * @param config - sending 'config' as a String and not as an org.w3c.dom.Element to avoid
    *        NotSerializableExceptions that show up when running tests on JDK 1.4.
    */
   void setSecurityConfig(String destName, String config) throws Exception;

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

}
