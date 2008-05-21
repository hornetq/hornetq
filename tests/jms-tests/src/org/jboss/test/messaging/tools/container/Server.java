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
package org.jboss.test.messaging.tools.container;

import org.jboss.kernel.spi.deployment.KernelDeployment;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.server.JMSServerManager;

import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.UserTransaction;
import java.rmi.Remote;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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

   
   /** Deploys a XML on the MicroContainer */
   KernelDeployment deployXML(String name, String xml) throws Exception;
   
   KernelDeployment deploy(String resource) throws Exception;

   void undeploy(KernelDeployment undeploy) throws Exception;

   Object getAttribute(ObjectName on, String attribute) throws Exception;

   void setAttribute(ObjectName on, String name, String valueAsString) throws Exception;

   Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception;

   void addNotificationListener(ObjectName on, NotificationListener listener) throws Exception;

   void removeNotificationListener(ObjectName on, NotificationListener listener) throws Exception;

   /**
    * Only for remote use!
    */
   void log(int level, String text) throws Exception;

   /**
    * @param serverPeerID - if null, the jboss-service.xml value will be used.
    * @param defaultQueueJNDIContext - if null, the jboss-service.xml value will be used.
    * @param defaultTopicJNDIContext - if null, the jboss-service.xml value will be used.
    */
   void startServerPeer(int serverPeerID,
                        String defaultQueueJNDIContext,
                        String defaultTopicJNDIContext,
                        ServiceAttributeOverrides attrOverrides,
                        boolean clustered) throws Exception;

   void stopServerPeer() throws Exception;

   boolean isServerPeerStarted() throws Exception;

   ObjectName getServerPeerObjectName() throws Exception;

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
//   void deployTopic(String name, String jndiName, boolean clustered) throws Exception;
//
//   /**
//    * Simulates a topic deployment (copying the topic descriptor in the deploy directory).
//    */
//   void deployTopic(String name, String jndiName, int fullSize, int pageSize,
//                    int downCacheSize, boolean clustered) throws Exception;
//
//   /**
//    * Creates a topic programatically.
//    */
//   void deployTopicProgrammatically(String name, String jndiName) throws Exception;
//
//   /**
//    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
//    */
//   void deployQueue(String name, String jndiName, boolean clustered) throws Exception;
//
//   /**
//    * Simulates a queue deployment (copying the queue descriptor in the deploy directory).
//    */
//   void deployQueue(String name, String jndiName, int fullSize, int pageSize,
//                    int downCacheSize, boolean clustered) throws Exception;
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
                                        boolean strictTck,
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

   /**
    * @param config - sending 'config' as a String and not as an org.w3c.dom.Element to avoid
    *        NotSerializableExceptions that show up when running tests on JDK 1.4.
    */
   void configureSecurityForDestination(String destName, boolean isQueue, HashSet<Role> roles) throws Exception;

   /**
    * Executes a command on the server
    * 
    * @param command
    * @return the return value
    * @throws Exception
    */
   Object executeCommand(Command command) throws Exception;

   UserTransaction getUserTransaction() throws Exception;

   /**
    * @return List<Notification>
    */
   List pollNotificationListener(long listenerID) throws Exception;

   void flushManagedConnectionPool() throws Exception;
   
   void deployConnectionFactory(String objectName, List<String> jndiBindings, boolean strictTck) throws Exception;

   MessagingServer getMessagingServer() throws Exception;

   InitialContext getInitialContext() throws Exception;

   void removeAllMessagesForQueue(String destName) throws Exception;

   void removeAllMessagesForTopic(String destName) throws Exception;

   Integer getMessageCountForQueue(String queueName) throws Exception;

   List listAllSubscriptionsForTopic(String s) throws Exception;

   HashSet<Role> getSecurityConfig() throws Exception;

   void setSecurityConfig(HashSet<Role> defConfig) throws Exception;

   void setSecurityConfigOnManager(boolean b, String s, HashSet<Role> lockedConf) throws Exception;

   void setRedeliveryDelayOnDestination(String dest, boolean queue, long delay) throws Exception;

   //void setDefaultRedeliveryDelay(long delay) throws Exception;
   JMSServerManager getJMSServerManager() throws Exception;
}
