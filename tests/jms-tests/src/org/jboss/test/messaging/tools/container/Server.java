/*
* JBoss, Home of Professional Open Source
* Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.test.messaging.tools.container;

import java.rmi.Remote;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.naming.InitialContext;

import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.server.JMSServerManager;

/**
 * The remote interface exposed by TestServer.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2868 $</tt>
 *
 * $Id: Server.java 2868 2007-07-10 20:22:16Z timfox $
 */
public interface Server extends Remote
{
   int getServerID() throws Exception;

   /**
    * @param attrOverrides - service attribute overrides that will take precedence over values
    *        read from configuration files.
    */
   void start(String[] containerConfig,
              HashMap<String, Object> configuration,
              boolean clearDatabase) throws Exception;

   /**
    * @return true if the server was stopped indeed, or false if the server was stopped already
    *         when the method was invoked.
    */
   boolean stop() throws Exception;

   /**
    * For a remote server, it "abruptly" kills the VM running the server. For a local server
    * it just stops the server.
    */
   void kill() throws Exception;

   /**
    * When kill is called you are actually schedulling the server to be killed in few milliseconds.
    * There are certain cases where we need to assure the server was really killed.
    * For that we have this simple ping we can use to verify if the server still alive or not.
    */
   void ping() throws Exception;

   /**
    * Only for remote use!
    */
   void log(int level, String text) throws Exception;

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    */
   void startServerPeer(int serverPeerID) throws Exception;

   void stopServerPeer() throws Exception;

   boolean isStarted() throws Exception;

   /**
    * Only for in-VM use!
    */
  // MessageStore getMessageStore() throws Exception;

   /**
    * Only for in-VM use!
    */
  // DestinationManager getDestinationManager() throws Exception;

//   StorageManager getPersistenceManager() throws Exception;
//
//   /**
//    * Only for in-VM use
//    */
   MessagingServer getServerPeer() throws Exception;

   void createQueue(String name, String jndiName) throws Exception;
   
   void destroyQueue(String name, String jndiName) throws Exception;
   
   void createTopic(String name, String jndiName) throws Exception;
   
   void destroyTopic(String name, String jndiName) throws Exception;
   
   
//   /**
//    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
//    */
//   void deployTopic(String name, String jndiName, boolean manageConfirmations) throws Exception;
//
//   /**
//    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
//    */
//   void deployTopic(String name, String jndiName, int fullSize, int pageSize,
//                    int downCacheSize, boolean manageConfirmations) throws Exception;
//
//   /**
//    * Creates a topic programatically.
//    */
//   void deployTopicProgrammatically(String name, String jndiName) throws Exception;
//
//   /**
//    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
//    */
//   void deployQueue(String name, String jndiName, boolean manageConfirmations) throws Exception;
//
//   /**
//    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
//    */
//   void deployQueue(String name, String jndiName, int fullSize, int pageSize,
//                    int downCacheSize, boolean manageConfirmations) throws Exception;
//
//   /**
//    * Creates a queue programatically.
//    */
//   void deployQueueProgrammatically(String name, String jndiName) throws Exception;

   /**
    * Simulates a destination un-deployment (deleting the destination descriptor from the deploy
    * directory).
    */
   //void undeployDestination(boolean isQueue, String name) throws Exception;

   /**
    * Destroys a programatically created destination.
    */
   //boolean undeployDestinationProgrammatically(boolean isQueue, String name) throws Exception;

   public void deployConnectionFactory(String clientId,
                                        String objectName,
                                        List<String> jndiBindings,
                                        int prefetchSize,
                                        int defaultTempQueueFullSize,
                                        int defaultTempQueuePageSize,
                                        int defaultTempQueueDownCacheSize,
                                        boolean supportsFailover,
                                        boolean supportsLoadBalancing,                             
                                        int dupsOkBatchSize,
                                        boolean blockOnAcknowledge) throws Exception;

   void deployConnectionFactory(String objectName,
                                List<String> jndiBindings,
                                int prefetchSize,
                                int defaultTempQueueFullSize,
                                int defaultTempQueuePageSize,
                                int defaultTempQueueDownCacheSize
                                ) throws Exception;
   
   void deployConnectionFactory(String objectName,
								         List<String> jndiBindings,
								         boolean supportsFailover,
								         boolean supportsLoadBalancing       
								         ) throws Exception;

   void deployConnectionFactory(String clientID,
                                String objectName,
                                List<String> jndiBindings) throws Exception;

   void deployConnectionFactory(String objectName,
                                List<String> jndiBindings,
                                int prefetchSize) throws Exception;

   void deployConnectionFactory(String objectName,
                                List<String> jndiBindings) throws Exception;

   void undeployConnectionFactory(String objectName) throws Exception;

   void configureSecurityForDestination(String destName, boolean isQueue, Set<Role> roles) throws Exception;

   MessagingServer getMessagingServer() throws Exception;

   InitialContext getInitialContext() throws Exception;

   void removeAllMessages(String destination, boolean isQueue) throws Exception;

   Integer getMessageCountForQueue(String queueName) throws Exception;

   List<String> listAllSubscribersForTopic(String s) throws Exception;

   Set<Role> getSecurityConfig() throws Exception;

   void setSecurityConfig(Set<Role> defConfig) throws Exception;

   //void setSecurityConfigOnManager(boolean b, String s, Set<Role> lockedConf) throws Exception;

   //void setDefaultRedeliveryDelay(long delay) throws Exception;
   JMSServerManager getJMSServerManager() throws Exception;
}
