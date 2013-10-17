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
package org.hornetq.core.client;

import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.protocol.core.Packet;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.w3c.dom.Node;

/**
 * Logger Code 21
 * <p>
 * Each message id must be 6 digits long starting with 10, the 3rd digit donates the level so
 *
 * <pre>
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 * </pre>
 *
 * so an INFO message would be 101000 to 101999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageLogger(projectCode = "HQ")
public interface HornetQClientLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQClientLogger LOGGER = Logger.getMessageLogger(HornetQClientLogger.class, HornetQClientLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211000, value = "**** Dumping session creation stacks ****", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStacks();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211001, value = "session created", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStack(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212000, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
   void warn(String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212001, value = "Error on clearing messages", format = Message.Format.MESSAGE_FORMAT)
   void errorClearingMessages(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212002, value = "Timed out waiting for handler to complete processing", format = Message.Format.MESSAGE_FORMAT)
   void timeOutWaitingForProcessing();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212003, value = "Unable to close session", format = Message.Format.MESSAGE_FORMAT)
   void unableToCloseSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212004, value = "Failed to connect to server.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212005, value = "Tried {0} times to connect. Now giving up on reconnecting it.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer(Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212006, value = "Waiting {0} milliseconds before next retry. RetryInterval={1} and multiplier={2}", format = Message.Format.MESSAGE_FORMAT)
   void waitingForRetry(Long interval, Long retryInterval, Double multiplier);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212007,
            value = "connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we will deal with it anyway."
               , format = Message.Format.MESSAGE_FORMAT)
   void createConnectorException(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212008,
            value = "I am closing a core ClientSessionFactory you left open. Please make sure you close all ClientSessionFactories explicitly "
                     + "before letting them go out of scope! {0}"
               , format = Message.Format.MESSAGE_FORMAT)
   void factoryLeftOpen(@Cause Exception e, int i);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212009, value = "resetting session after failure", format = Message.Format.MESSAGE_FORMAT)
   void resettingSessionAfterFailure();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212010, value = "Server is starting, retry to create the session {0}", format = Message.Format.MESSAGE_FORMAT)
   void retryCreateSessionSeverStarting(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212011, value = "committing transaction after failover occurred, any non persistent messages may be lost", format = Message.Format.MESSAGE_FORMAT)
   void commitAfterFailover();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212012, value = "failover occured during commit throwing XAException.XA_RETRY", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringCommit();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212013, value = "failover occurred during prepare re-trying", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringPrepare();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212014, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringPrepareRollingBack();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212015, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void errorDuringPrepare(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212016,
            value = "I am closing a core ClientSession you left open. Please make sure you close all ClientSessions explicitly before letting them go out of scope! {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void clientSessionNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212017, value = "error adding packet", format = Message.Format.MESSAGE_FORMAT)
   void errorAddingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212018, value = "error calling cancel", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingCancel(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212019, value = "error reading index", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212020, value = "error setting index", format = Message.Format.MESSAGE_FORMAT)
   void errorSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212021, value = "error resetting index", format = Message.Format.MESSAGE_FORMAT)
   void errorReSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212022, value = "error reading LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212023, value = "error closing LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212024, value = "Exception during finalization for LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorFinalisingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212025, value = "did not connect the cluster connection to other nodes", format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingToNodes(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212026, value = "Timed out waiting for pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212027, value = "Timed out waiting for scheduled pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForScheduledPoolTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212028, value = "error starting server locator", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingLocator(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212029,
            value = "Closing a Server Locator left open. Please make sure you close all Server Locators explicitly before letting them go out of scope! {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void serverLocatorNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212030, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopology(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212031, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopologyNodedown(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212032, value = "Timed out waiting to stop discovery thread", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingDiscovery();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212033, value = "unable to send notification when discovery group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingNotifOnDiscoveryStop(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212034, value = "There are more than one servers on the network broadcasting the same node id. "
                     + "You will see this message exactly once (per node) if a node is restarted, in which case it can be safely "
                     + "ignored. But if it is logged continuously it means you really do have more than one node on the same network "
                     + "active concurrently with the same node id. This could occur if you have a backup node active at the same time as "
                     + "its live node. nodeID={0}",
         format = Message.Format.MESSAGE_FORMAT)
   void multipleServersBroadcastingSameNode(String nodeId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212035, value = "error receiving packet in discovery", format = Message.Format.MESSAGE_FORMAT)
   void errorReceivingPAcketInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212036,
            value = "Can not find packet to clear: {0} last received command id first stored command id {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void cannotFindPacketToClear(Integer lastReceivedCommandID, Integer firstStoredCommandID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212037, value = "Connection failure has been detected: {0} [code={1}]", format = Message.Format.MESSAGE_FORMAT)
   void connectionFailureDetected(String message, HornetQExceptionType type);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212038, value = "Failure in calling interceptor: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingInterceptor(@Cause Throwable e, Interceptor interceptor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212040, value = "Timed out waiting for netty ssl close future to complete", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingSSL();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212041, value = "Timed out waiting for netty channel to close", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingNettyChannel();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212042, value = "Timed out waiting for packet to be flushed", format = Message.Format.MESSAGE_FORMAT)
   void timeoutFlushingPacket();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212043, value = "Property {0} must be an Integer, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotInteger(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212044, value = "Property {0} must be an Long, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotLong(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212045, value = "Property {0} must be an Boolean, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotBoolean(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212046, value = "Cannot find hornetq-version.properties on classpath: {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void noVersionOnClasspath(String classpath);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212047, value = "Warning: JVM allocated more data what would make results invalid {0}:{1}", format = Message.Format.MESSAGE_FORMAT)
   void jvmAllocatedMoreMemory(Long totalMemory1, Long totalMemory2);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212048, value = "local-bind-address specified for broadcast group but no local-bind-port specified so socket will NOT be bound to a local address/port",
         format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupBindError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212049, value = "Could not bind to {0} ({1} address); " +
         "make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6)." +
         "\nIgnoring discovery group-address, but this may lead to cross talking.",
         format = Message.Format.MESSAGE_FORMAT)
   void ioDiscoveryError(String hostAddress, String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212050, value = "Compressed large message tried to read {0} bytes from stream {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void compressedLargeMessageError(int length, int nReadBytes);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214000, value = "Failed to call onMessage", format = Message.Format.MESSAGE_FORMAT)
   void onMessageError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214001, value = "failed to cleanup session", format = Message.Format.MESSAGE_FORMAT)
   void failedToCleanupSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214002, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToExecuteListener(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214003, value = "Failed to handle failover", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandleFailover(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214004, value = "XA end operation failed ", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingEnd(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214005, value = "XA start operation failed {0} code:{1}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingStart(String message, Integer code);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214006, value = "Session is not XA", format = Message.Format.MESSAGE_FORMAT)
   void sessionNotXA();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214007, value = "Received exception asynchronously from server", format = Message.Format.MESSAGE_FORMAT)
   void receivedExceptionAsynchronously(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214008, value = "Failed to handle packet", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandlePacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214009, value = "Failed to stop discovery group", format = Message.Format.MESSAGE_FORMAT)
   void failedToStopDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214010, value = "Failed to receive datagram", format = Message.Format.MESSAGE_FORMAT)
   void failedToReceiveDatagramInDiscovery(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214011, value = "Failed to call discovery listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToCallListenerInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214012, value = "Unexpected error handling packet {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingPacket(@Cause Throwable t, Packet packet);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214013, value = "Failed to decode packet", format = Message.Format.MESSAGE_FORMAT)
   void errorDecodingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214014, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingFailureListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214015, value = "Failed to execute connection life cycle listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingLifeCycleListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214016, value = "Failed to create netty connection", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingNettyConnection(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214017, value =  "Caught unexpected Throwable", format = Message.Format.MESSAGE_FORMAT)
   void caughtunexpectedThrowable(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214018, value =  "Failed to invoke getTextContent() on node {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransform(@Cause Throwable t, Node n);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214019, value =  "Invalid configuration", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransformInvalidConf(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214020, value = "Exception happened while stopping Discovery BroadcastEndpoint {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingDiscoveryBroadcastEndpoint(Object endpoint, @Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 214021,
      value = "Invalid concurrent session usage. Sessions are not supposed to be used by more than one thread concurrently.",
      format = Message.Format.MESSAGE_FORMAT)
   void invalidConcurrentSessionUsage(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 214022,
      value = "Packet {0} was answered out of sequence due to a previous server timeout and it's being ignored",
      format = Message.Format.MESSAGE_FORMAT)
   void packetOutOfOrder(Object obj, @Cause Throwable t);


   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 214024,
            value = "Destination address={0} is blocked. If the system is configured to block make sure you consume messages on this configuration.",
            format = Message.Format.MESSAGE_FORMAT)
   void outOfCreditOnFlowControl (String address);

}
