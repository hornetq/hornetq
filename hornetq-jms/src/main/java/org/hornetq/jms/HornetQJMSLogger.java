/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.jms;

import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.w3c.dom.Node;

import javax.management.ObjectName;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/15/12
 *
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 12, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 121000 to 121999
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQJMSLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQJMSLogger LOGGER = Logger.getMessageLogger(HornetQJMSLogger.class, HornetQJMSLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121001, value = "Failed to set up JMS bridge connections. Most probably the source or target servers are unavailable." +
         " Will retry after a pause of {0} ms", format = Message.Format.MESSAGE_FORMAT)
   void failedToSetUpBridge(long failureRetryInterval);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121002, value = "JMS Bridge Succeeded in reconnecting to servers" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeReconnected();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121003, value = "Succeeded in connecting to servers" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnected();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121004, value = "JMS Server Manager Running cached command for {0}" , format = Message.Format.MESSAGE_FORMAT)
   void serverRunningCachedCommand(Runnable run);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 121004, value = "JMS Server Manager Caching command for {0} since the JMS Server is not active yet" ,
         format = Message.Format.MESSAGE_FORMAT)
   void serverCachingCommand(Object runnable);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122001, value = "Attempt to start JMS Bridge, but is already started" , format = Message.Format.MESSAGE_FORMAT)
   void errorBridgeAlreadyStarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122002, value = "Failed to start JMS Bridge" , format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122003, value = "Failed to unregisted JMS Bridge {0}" , format = Message.Format.MESSAGE_FORMAT)
   void errorUnregisteringBridge(ObjectName objectName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122004, value = "JMS Bridge unable to set up connections, bridge will be stopped" , format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122005, value = "JMS Bridge Will retry after a pause of {0} ms" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeRetry(long failureRetryInterval);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122006, value = "JMS Bridge unable to set up connections, bridge will not be started" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotStarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122007, value = "Detected failure on bridge connection" , format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailure();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122008, value = "I'm closing a JMS connection you left open. Please make sure you close all JMS connections explicitly before letting them go out of scope!" +
   "see stacktrace to find out where it was created" , format = Message.Format.MESSAGE_FORMAT)
   void connectionLeftOpen(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122009, value = "Queue {0} doesn't exist on the topic {1}. It was deleted manually probably." , format = Message.Format.MESSAGE_FORMAT)
   void noQueueOnTopic(String queueName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122010, value = "XA Recovery Can't connect to any hornetq server on recovery {0}"  , format = Message.Format.MESSAGE_FORMAT)
   void recoveryConnectFailed(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122011, value = "JMS Bridge failed to send + acknowledge batch, closing JMS objects"  , format = Message.Format.MESSAGE_FORMAT)
   void bridgeAckError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122012, value = "Failed to connect JMS Bridge", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnectError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122013, value = "Unhandled exception thrown from onMessage" , format = Message.Format.MESSAGE_FORMAT)
   void onMessageError(@Cause Exception e);

      @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122014, value = "error unbinding {0} from JNDI" , format = Message.Format.MESSAGE_FORMAT)
   void jndiUnbindError(@Cause Exception e, String key);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122015, value = "JMS Server Manager error" , format = Message.Format.MESSAGE_FORMAT)
   void jmsServerError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122016, value = "Error in XA Recovery recover" , format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122017, value = "Notified of connection failure in xa recovery connectionFactory for provider {0} will attempt reconnect on next pass" ,
         format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverConnectionError(@Cause Exception e, ClientSessionFactory csf);

      @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122018, value = "Can't connect to {0} on auto-generated resource recovery" ,
         format = Message.Format.MESSAGE_FORMAT)
   void xaRecoverAutoConnectionError(@Cause Throwable e, XARecoveryConfig csf);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 122019, value = "Error in XA Recovery" , format = Message.Format.MESSAGE_FORMAT)
   void xaRecoveryError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124001, value = "key attribute missing for JMS configuration {0}" , format = Message.Format.MESSAGE_FORMAT)
   void jmsConfigMissingKey(Node e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124002, value = "Failed to start source connection" , format = Message.Format.MESSAGE_FORMAT)
   void jmsBridgeSrcConnectError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124003, value = "Failed to start JMS deployer" , format = Message.Format.MESSAGE_FORMAT)
   void jmsDeployerStartError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124004, value = "Failed to call JMS exception listener" , format = Message.Format.MESSAGE_FORMAT)
   void errorCallingExcListener(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124005, value = "Consumer Failed to prepare message for delivery", format = Message.Format.MESSAGE_FORMAT)
   void errorPreparingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124006, value = "Queue Browser failed to create message" , format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124007, value = "Message Listener failed to prepare message for receipt" , format = Message.Format.MESSAGE_FORMAT)
   void errorPreparingMessageForReceipt(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124008, value = "Message Listener failed to process message" , format = Message.Format.MESSAGE_FORMAT)
   void errorProcessingMessage(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 124008, value = "Message Listener failed to recover session" , format = Message.Format.MESSAGE_FORMAT)
   void errorRecoveringSession(@Cause Throwable e);
}
