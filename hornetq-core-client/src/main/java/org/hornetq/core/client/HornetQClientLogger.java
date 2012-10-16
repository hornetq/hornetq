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

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/8/12
 *
 * Logger Code 21
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 101000 to 101999
 */

import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.Pair;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Logger;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;
import org.jboss.netty.channel.Channel;
import org.w3c.dom.Node;

import javax.transaction.xa.Xid;
import java.io.File;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@MessageLogger(projectCode = "HQ")
public interface HornetQClientLogger extends BasicLogger
{
   /**
    * The default logger.
    */
   HornetQClientLogger LOGGER = Logger.getMessageLogger(HornetQClientLogger.class, HornetQClientLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211014, value = "**** Dumping session creation stacks ****", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStacks();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 211015, value = "session created", format = Message.Format.MESSAGE_FORMAT)
   void dumpingSessionStack(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212007, value = "Binding already exists with name {0}, divert will not be deployed", format = Message.Format.MESSAGE_FORMAT)
   void divertBindingNotExists(SimpleString bindingName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212008, value = "Security risk! HornetQ is running with the default cluster admin user and default password. "
         + "Please see the HornetQ user guide, cluster chapter, for instructions on how to change this.", format = Message.Format.MESSAGE_FORMAT)
   void clusterSecurityRisk();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212009, value = "unable to restart server, please kill and restart manually", format = Message.Format.MESSAGE_FORMAT)
   void serverRestartWarning();

   @LogMessage(level = Logger.Level.WARN)
   void serverRestartWarning(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212010, value = "Unable to announce backup for replication. Trying to stop the server.", format = Message.Format.MESSAGE_FORMAT)
   void replicationStartProblem(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212011, value = "Critical IO Error, shutting down the server. code={0}, message={1}", format = Message.Format.MESSAGE_FORMAT)
   void ioErrorShutdownServer(HornetQExceptionType code, String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212012, value = "Error stopping server", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingServer(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212013, value = "Timed out waiting for backup activation to exit", format = Message.Format.MESSAGE_FORMAT)
   void backupActivationProblem();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212014, value = "Error when trying to start replication", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212015, value = "Error when trying to stop replication", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212016, value = "{0}", format = Message.Format.MESSAGE_FORMAT)
   void warn(String message);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212017, value = "Error on clearing messages", format = Message.Format.MESSAGE_FORMAT)
   void errorClearingMessages(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212018, value = "Timed out waiting for handler to complete processing", format = Message.Format.MESSAGE_FORMAT)
   void timeOutWaitingForProcessing();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212019, value = "Unable to close session", format = Message.Format.MESSAGE_FORMAT)
   void unableToCloseSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212020, value = "Failed to connect to server.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212021, value = "Tried {0} times to connect. Now giving up on reconnecting it.", format = Message.Format.MESSAGE_FORMAT)
   void failedToConnectToServer(Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212022, value = "Waiting {0} milliseconds before next retry. RetryInterval={1} and multiplier={2}", format = Message.Format.MESSAGE_FORMAT)
   void waitingForRetry(Long interval, Long retryInterval, Double multiplier);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212023,
            value = "connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we will deal with it anyway."
               , format = Message.Format.MESSAGE_FORMAT)
   void createConnectorException(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212024,
            value = "I am closing a core ClientSessionFactory you left open. Please make sure you close all ClientSessionFactories explicitly "
                     + "before letting them go out of scope! {0}"
               , format = Message.Format.MESSAGE_FORMAT)
   void factoryLeftOpen(@Cause Exception e, int i);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212025, value = "resetting session after failure", format = Message.Format.MESSAGE_FORMAT)
   void resettingSessionAfterFailure();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212026, value = "Server is starting, retry to create the session {0}", format = Message.Format.MESSAGE_FORMAT)
   void retryCreateSessionSeverStarting(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212027, value = "committing transaction after failover occurred, any non persistent messages may be lost", format = Message.Format.MESSAGE_FORMAT)
   void commitAfterFailover();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212028, value = "failover occured during commit throwing XAException.XA_RETRY", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringCommit();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212029, value = "failover occurred during prepare re-trying", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringPrepare();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212030, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void failoverDuringPrepareRollingBack();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212031, value = "failover occurred during prepare rolling back", format = Message.Format.MESSAGE_FORMAT)
   void errorDuringPrepare(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212032,
            value = "I am closing a core ClientSession you left open. Please make sure you close all ClientSessions explicitly before letting them go out of scope! {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void clientSessionNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212033, value = "error adding packet", format = Message.Format.MESSAGE_FORMAT)
   void errorAddingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212034, value = "error calling cancel", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingCancel(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212035, value = "error reading index", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212036, value = "error setting index", format = Message.Format.MESSAGE_FORMAT)
   void errorSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212037, value = "error resetting index", format = Message.Format.MESSAGE_FORMAT)
   void errorReSettingIndex(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212038, value = "error reading LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212039, value = "error closing LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212040, value = "Exception during finalization for LargeMessage file cache", format = Message.Format.MESSAGE_FORMAT)
   void errorFinalisingCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212041, value = "did not connect the cluster connection to other nodes", format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingToNodes(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212042, value = "Timed out waiting for pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212043, value = "Timed out waiting for scheduled pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingForScheduledPoolTermination();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212044, value = "error starting server locator", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingLocator(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212045,
            value = "Closing a Server Locator left open. Please make sure you close all Server Locators explicitly before letting them go out of scope! {0}",
            format = Message.Format.MESSAGE_FORMAT)
   void serverLocatorNotClosed(@Cause Exception e, int identity);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212046, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopology(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212047, value = "error sending topology", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingTopologyNodedown(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212048, value = "Timed out waiting to stop discovery thread", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingDiscovery();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212049, value = "unable to send notification when discovery group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingNotifOnDiscoveryStop(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212050, value = "There are more than one servers on the network broadcasting the same node id. "
                     + "You will see this message exactly once (per node) if a node is restarted, in which case it can be safely "
                     + "ignored. But if it is logged continuously it means you really do have more than one node on the same network "
                     + "active concurrently with the same node id. This could occur if you have a backup node active at the same time as "
                     + "its live node. nodeID={0}",
         format = Message.Format.MESSAGE_FORMAT)
   void multipleServersBroadcastingSameNode(String nodeId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212051, value = "error receiving packet in discovery", format = Message.Format.MESSAGE_FORMAT)
   void errorReceivingPAcketInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212052, value = "Cannot deploy a connector with no name specified.", format = Message.Format.MESSAGE_FORMAT)
   void connectorWithNoName();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212053, value = "There is already a connector with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void connectorAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212054,
            value = "AIO was not located on this platform, it will fall back to using pure Java NIO. If your platform is Linux, install LibAIO to enable the AIO journal",
            format = Message.Format.MESSAGE_FORMAT)
   void AIONotFound();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212055, value = "There is already a discovery group with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void discoveryGroupAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212056, value = "error scanning for URL's", format = Message.Format.MESSAGE_FORMAT)
   void errorScanningURLs(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212057, value = "problem undeploying {0}", format = Message.Format.MESSAGE_FORMAT)
   void problemUndeployingNode(@Cause Exception e, Node node);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212058, value = "Timed out waiting for paging cursor to stop {0} {1}", format = Message.Format.MESSAGE_FORMAT)
   void timedOutStoppingPagingCursor(FutureLatch future, Executor executor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212059, value = "Timed out flushing executors for paging cursor to stop {0} {1}", format = Message.Format.MESSAGE_FORMAT)
   void timedOutFlushingExecutorsPagingCursor(FutureLatch future, Executor executor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212060, value = "problem cleaning page address {0}", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningPageAddress(@Cause Exception e, SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212062, value = "Problem cleaning page subscription counter", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningPagesubscriptionCounter(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212063, value = "Error on cleaning up cursor pages", format = Message.Format.MESSAGE_FORMAT)
   void problemCleaningCursorPages(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212064, value = "Could not remove page {0} from consumed pages on cursor for address {1}",
            format = Message.Format.MESSAGE_FORMAT)
   void problemRemovingCursorPages(Long pageId, SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212068, value = "File {0} being renamed to {1}.invalidPage as it was loaded partially. Please verify your data.", format = Message.Format.MESSAGE_FORMAT)
   void pageInvalid(String fileName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212069, value = "Error while deleting page file", format = Message.Format.MESSAGE_FORMAT)
   void pageDeleteError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212070, value = "page finalise error", format = Message.Format.MESSAGE_FORMAT)
   void pageFinaliseError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212071, value = "Page file had incomplete records at position {0} at record number {1}", format = Message.Format.MESSAGE_FORMAT)
   void pageSuspectFile(int position, int msgNumber);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212072, value = "Can not delete page transaction id={0}", format = Message.Format.MESSAGE_FORMAT)
   void pageTxDeleteError(@Cause Exception e, long recordID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212073, value = "Directory {0} did not have an identification file {1}",
            format = Message.Format.MESSAGE_FORMAT)
   void pageStoreFactoryNoIdFile(String s, String addressFile);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212074, value = "Timed out on waiting PagingStore {0} to shutdown", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreTimeout(SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212075, value = "IO Error, impossible to start paging", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreStartIOError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212076, value = "Starting paging on {0}, size = {1}, maxSize={2}", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreStart(SimpleString storeName, long addressSize, long maxSize);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212077, value = "Messages are being dropped on address {0}", format = Message.Format.MESSAGE_FORMAT)
   void pageStoreDropMessages(SimpleString storeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212078, value = "Server is stopped", format = Message.Format.MESSAGE_FORMAT)
   void serverIsStopped();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212079, value = "Cannot find queue {0} to update delivery count", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueDelCount(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212080, value = "Cannot find message {0} to update delivery count", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindMessageDelCount(Long msg);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212081, value = "Message for queue {0} which does not exist. This message will be ignored.", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueForMessage(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212082, value = "It was not possible to delete message {0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorDeletingMessage(@Cause Exception e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212083, value = "Message in prepared tx for queue {0} which does not exist. This message will be ignored.", format = Message.Format.MESSAGE_FORMAT)
   void journalMessageInPreparedTX(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212084, value = "Failed to remove reference for {0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorRemovingRef(Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212085, value = "Can not find queue {0} while reloading ACKNOWLEDGE_CURSOR",
            format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueReloadingACK(Long queueID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212086, value = "PAGE_CURSOR_COUNTER_VALUE record used on a prepared statement, invalid state", format = Message.Format.MESSAGE_FORMAT)
   void journalPAGEOnPrepared();

   @LogMessage(level = Logger.Level.WARN)
   @Message(
            id = 212087,
            value = "InternalError: Record type {0} not recognized. Maybe you are using journal files created on a different version",
            format = Message.Format.MESSAGE_FORMAT)
   void journalInvalidRecordType(Byte recordType);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212088, value = "Can not locate recordType={0} on loadPreparedTransaction//deleteRecords",
            format = Message.Format.MESSAGE_FORMAT)
   void journalInvalidRecordTypeOnPreparedTX(Byte recordType);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212089, value = "Journal Error", format = Message.Format.MESSAGE_FORMAT)
   void journalError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212090, value = "error incrementing delay detection", format = Message.Format.MESSAGE_FORMAT)
   void errorIncrementDelayDeletionCount(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212092, value = "Error on executing IOAsyncTask", format = Message.Format.MESSAGE_FORMAT)
   void errorExecutingIOAsyncTask(@Cause Throwable t);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212093, value = "Error on deleting duplicate cache", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingDuplicateCache(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212094, value = "Reaper thread being restarted", format = Message.Format.MESSAGE_FORMAT)
   void reaperRestarted();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212095, value = "Did not route to any bindings for address {0} and sendToDLAOnNoRoute is true " +
                                 "but there is no DLA configured for the address, the message will be ignored.",
         format = Message.Format.MESSAGE_FORMAT)
   void noDLA(SimpleString address);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212096, value = "It was not possible to add references due to an IO error code {0} message = {1}",
            format = Message.Format.MESSAGE_FORMAT)
   void ioErrorAddingReferences(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212099, value = "Error while confirming large message completion on rollback for recordID={0}", format = Message.Format.MESSAGE_FORMAT)
   void journalErrorConfirmingLargeMessage(@Cause Throwable e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212100, value = "Client connection failed, clearing up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clientConnectionFailed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212101, value = "Cleared up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clearingUpSession(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212102, value = "Error processing IOCallback code = {0} message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void errorProcessingIOCallback(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212103,
            value = "Can not find packet to clear: {0} last received command id first stored command id {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void cannotFindPacketToClear(Integer lastReceivedCommandID, Integer firstStoredCommandID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212104, value = "Client with version {0} and address {1} is not compatible with server version {2}. " +
                     "Please ensure all clients and servers are upgraded to the same version for them to interoperate properly",
         format = Message.Format.MESSAGE_FORMAT)
   void incompatibleVersion(Integer version, String remoteAddress, String fullVersion);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212105, value = "Client is not being consistent on the request versioning. It just sent a version id={0}" +
            		   " while it informed {1} previously", format = Message.Format.MESSAGE_FORMAT)
   void incompatibleVersionAfterConnect(int version, int clientVersion);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212106, value = "Reattach request from {0} failed as there is no confirmationWindowSize configured, which may be ok for your system", format = Message.Format.MESSAGE_FORMAT)
   void reattachRequestFailed(String remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212107, value = "Connection failure has been detected: {0} [code={1}]", format = Message.Format.MESSAGE_FORMAT)
   void connectionFailureDetected(String message, HornetQExceptionType type);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212108, value = "Failure in calling interceptor: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingInterceptor(@Cause Throwable e, Interceptor interceptor);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212109, value = "error cleaning up stomp connection", format = Message.Format.MESSAGE_FORMAT)
   void errorCleaningStompConn(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212110, value = "Stomp Transactional acknowledgement is not supported", format = Message.Format.MESSAGE_FORMAT)
   void stompTXAckNorSupported();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212111, value = "Interrupted while waiting for stomp heart beate to die", format = Message.Format.MESSAGE_FORMAT)
   void errorOnStompHeartBeat(@Cause InterruptedException e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212112, value = "Timed out flushing channel on InVMConnection", format = Message.Format.MESSAGE_FORMAT)
   void timedOutFlushingInvmChannel();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212113, value = "Unexpected Netty Version was expecting {0} using {1} Version.ID", format = Message.Format.MESSAGE_FORMAT)
   void unexpectedNettyVersion(String nettyVersion, String id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212114, value = "channel group did not completely close", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelGroupError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212116, value = "{0} is still connected to {1}", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelStillOpen(Channel channel, SocketAddress remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212117, value = "channel group did not completely unbind", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelGroupBindError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212118, value = "{0} is still bound to {1}", format = Message.Format.MESSAGE_FORMAT)
   void nettyChannelStillBound(Channel channel, SocketAddress remoteAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212119, value = "Timed out waiting for netty ssl close future to complete", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingSSL();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212120, value = "Timed out waiting for netty channel to close", format = Message.Format.MESSAGE_FORMAT)
   void timeoutClosingNettyChannel();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212121, value = "Timed out waiting for packet to be flushed", format = Message.Format.MESSAGE_FORMAT)
   void timeoutFlushingPacket();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212122, value = "Error instantiating remoting interceptor {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingRemotingInterceptor(@Cause Exception e, String interceptorClass);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212123, value = "The following keys are invalid for configuring the acceptor: {0} the acceptor will not be started.",
         format = Message.Format.MESSAGE_FORMAT)
   void invalidAcceptorKeys(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212124, value = "Error instantiating remoting acceptor {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingAcceptor(@Cause Exception e, String factoryClassName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212125, value = "Timed out waiting for remoting thread pool to terminate", format = Message.Format.MESSAGE_FORMAT)
   void timeoutRemotingThreadPool();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212126, value = "error on connection failure check", format = Message.Format.MESSAGE_FORMAT)
   void errorOnFailureCheck(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212127, value = "The following keys are invalid for configuring the connector service: {0} the connector will not be started.",
         format = Message.Format.MESSAGE_FORMAT)
   void connectorKeysInvalid(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212128, value = "The following keys are required for configuring the connector service: {0} the connector will not be started.",
         format = Message.Format.MESSAGE_FORMAT)
   void connectorKeysMissing(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212129, value = "Packet {0} can not be processed by the ReplicationEndpoint",
            format = Message.Format.MESSAGE_FORMAT)
   void invalidPacketForReplication(Packet packet);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212130, value = "error handling packet {0} for replication", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingReplicationPacket(@Cause Exception e, Packet packet);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212131, value = "Replication Error while closing the page on backup", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingPageOnReplication(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212132, value = "Journal comparison mismatch:\n{0}", format = Message.Format.MESSAGE_FORMAT)
   void journalcomparisonMismatch(String s);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212133, value = "Replication Error deleting large message ID = {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingLargeMessage(@Cause Exception e, long messageId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212134, value = "Replication Large MessageID {0}  is not available on backup server. Ignoring replication message", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageNotAvailable(long messageId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212135, value = "Error completing callback on replication manager", format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingReplicationCallback(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212136, value = "The backup node has been shut-down, replication will now stop", format = Message.Format.MESSAGE_FORMAT)
   void replicationStopOnBackupShutdown();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212137, value = "Connection to the backup node failed, removing replication now", format = Message.Format.MESSAGE_FORMAT)
   void replicationStopOnBackupFail(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212138, value = "Timed out waiting to stop Bridge", format = Message.Format.MESSAGE_FORMAT)
   void timedOutWaitingToStopBridge();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212139, value = "Bridge unable to send notification when broadcast group is stopped", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotificationOnGroupStopped(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212141, value = "Connection failed with failedOver={1}", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnectionFailed(@Cause Exception e, Boolean failedOver);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212142, value = "Error on querying binding on bridge {0}. Retrying in 100 milliseconds", format = Message.Format.MESSAGE_FORMAT)
   void errorQueryingBridge(@Cause Throwable t, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212143, value = "Address {0} does not have any bindings yet, retry #({1})",
            format = Message.Format.MESSAGE_FORMAT)
   void errorQueryingBridge(SimpleString address, Integer retryCount);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212144, value = "Server is starting, retry to create the session for bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBridge(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212146, value = "ServerLocator was shutdown, can not retry on opening connection for bridge",
            format = Message.Format.MESSAGE_FORMAT)
   void bridgeLocatorShutdown();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212147, value =  "Bridge {0} achieved {1} maxattempts={2} it will stop retrying to reconnect", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAbortStart(SimpleString name, Integer retryCount, Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212148, value = "Unexpected exception while trying to reconnect", format = Message.Format.MESSAGE_FORMAT)
   void errorReConnecting(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212149, value = "transaction with xid {0} timed out", format = Message.Format.MESSAGE_FORMAT)
   void unexpectedXid(Xid xid);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212150, value = "IO Error completing the transaction, code = {0}, message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void ioErrorOnTX(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212151, value = "Property {0} must be an Integer, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotInteger(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212152, value = "Property {0} must be an Long, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotLong(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212153, value = "Property {0} must be an Boolean, it is {1}", format = Message.Format.MESSAGE_FORMAT)
   void propertyNotBoolean(String propName, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212154, value = "Cannot find hornetq-version.properties on classpath: {1}", format = Message.Format.MESSAGE_FORMAT)
   void noVersionOnClasspath(String classpath);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212155, value = "Warning: JVM allocated more data what would make results invalid {0}:{1}", format = Message.Format.MESSAGE_FORMAT)
   void jvmAllocatedMoreMemory(Long totalMemory1, Long totalMemory2);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212156, value = "Could not finish context execution in 10 seconds",
            format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingContext(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212157, value = "Replacing incomplete LargeMessage with ID={0}", format = Message.Format.MESSAGE_FORMAT)
   void replacingIncompleteLargeMessage(Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212158, value = "Cleared up resources for session {0}", format = Message.Format.MESSAGE_FORMAT)
   void clientConnectionFailedClearingSession(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212159, value = "local-bind-address specified for broadcast group but no local-bind-port specified so socket will NOT be bound to a local address/port",
         format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupBindError();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212160, value = "unable to send notification when broadcast group is stopped",
         format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupClosed(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212161, value = "NodeID={0} is not available on the topology. Retrying the connection to that node now", format = Message.Format.MESSAGE_FORMAT)
   void nodeNotAvailable(String targetNodeID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212163, value = "exception while invoking {0} on {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void managementOperationError(@Cause Exception e, String op, String resourceName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212164, value = "exception while retrieving attribute {0} on {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void managementAttributeError(@Cause Exception e, String att, String resourceName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212165, value = "On ManagementService stop, there are {0} unexpected registered MBeans: {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void managementStopError(Integer size, List<String> unexpectedResourceNames);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212166, value = "Unable to delete group binding info {0}",
         format = Message.Format.MESSAGE_FORMAT)
   void unableToDeleteGroupBindings(@Cause Exception e, SimpleString groupId);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212167, value = "Error closing serverLocator={0}",
         format = Message.Format.MESSAGE_FORMAT)
   void errorClosingServerLocator(@Cause Exception e, ServerLocatorInternal clusterLocator);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212168, value = "unable to start broadcast group {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartBroadcastGroup(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212169, value = "unable to start cluster connection {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartClusterConnection(@Cause Exception e, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212170, value = "unable to start Bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToStartBridge(@Cause Exception e, SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212171, value = "No connector with name '{0}'. backup cannot be announced.", format = Message.Format.MESSAGE_FORMAT)
   void announceBackupNoConnector(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212172, value = "no cluster connections defined, unable to announce backup", format = Message.Format.MESSAGE_FORMAT)
   void announceBackupNoClusterConnections();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212173, value =  "Must specify a unique name for each bridge. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotUnique();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212174, value =  "Must specify a queue name for each bridge. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoQueue();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212175, value =  "Bridge Forward address is not specified. Will use original message address instead", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoForwardAddress();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212176, value =  "There is already a bridge with name {0} deployed. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAlreadyDeployed(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212177, value =   "No queue found with name {0} bridge will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoQueue(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212178, value =   "No discovery group found with name {0} bridge will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoDiscoveryGroup(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212179, value =  "Must specify a unique name for each cluster connection. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNotUnique();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212180, value =  "Must specify an address for each cluster connection. This one will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoForwardAddress();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212181, value =  "No connector with name '{0}'. The cluster connection will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoConnector(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212182, value =   "Cluster Configuration  '{0}' already exists. The cluster connection will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionAlreadyExists(String connectorName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212183, value =   "No discovery group with name '{0}'. The cluster connection will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
   void clusterConnectionNoDiscoveryGroup(String discoveryGroupName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212184, value =   "There is already a broadcast-group with name {0} deployed. This one will not be deployed." , format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupAlreadyExists(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212185, value =   "There is no connector deployed with name '{0}'. The broadcast group with name '{1}' will not be deployed."  , format = Message.Format.MESSAGE_FORMAT)
   void broadcastGroupNoConnector(String connectorName, String bgName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212186, value =   "No connector defined with name '{0}'. The bridge will not be deployed.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNoConnector(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212187, value =   "Stopping Redistributor, Timed out waiting for tasks to complete", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingRedistributor();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212188, value =   "IO Error during redistribution, errorCode = {0} message = {1}", format = Message.Format.MESSAGE_FORMAT)
   void ioErrorRedistributing(Integer errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212189, value =   "Unable to announce backup, retrying", format = Message.Format.MESSAGE_FORMAT)
   void errorAnnouncingBackup(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212191, value =   "{0}::Remote queue binding {1} has already been bound in the post office. Most likely cause for this is you have a loop " +
                                   "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses",
         format = Message.Format.MESSAGE_FORMAT)
   void remoteQueueAlreadyBoundOnClusterConnection(Object messageFlowRecord, SimpleString clusterName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212192, value =   "problem closing backup session factory for cluster connection", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingBackupFactoryOnClusterConnection(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212193, value = "Node Manager can not open file {0}", format = Message.Format.MESSAGE_FORMAT)
   void nodeManagerCantOpenFile(@Cause Exception e, File file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212194, value =   "Error on resetting large message deliver - {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorResttingLargeMessage(@Cause Throwable e, Object deliverer);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212195, value =   "Timed out waiting for executor to complete", format = Message.Format.MESSAGE_FORMAT)
   void errorTransferringConsumer();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212196, value = "Queue could not finish waiting executors. Try increasing the thread pool size",
            format = Message.Format.MESSAGE_FORMAT)
   void errorFlushingExecutorsOnQueue();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212198, value =   "Message has expired. No bindings for Expiry Address {0} so dropping it", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringReferencesNoBindings(SimpleString expiryAddress);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212199, value =   "Message has expired. No expiry queue configured for queue {0} so dropping it", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringReferencesNoQueue(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212202, value =     "Message has exceeded max delivery attempts. No Dead Letter Address configured for queue {0} so dropping it",
         format = Message.Format.MESSAGE_FORMAT)
   void messageExceededMaxDeliveryNoDLA(SimpleString name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212204, value =     "Unable to decrement reference counting on queue" ,
         format = Message.Format.MESSAGE_FORMAT)
   void errorDecrementingRefCount(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212205, value =     "Unable to remove message id = {0} please remove manually" ,
         format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingMessage(@Cause Throwable e, Long messageID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212206, value =     "Error checking DLQ" ,
         format = Message.Format.MESSAGE_FORMAT)
   void errorCheckingDLQ(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212207, value =     "Failed to register as backup. Stopping the server."  ,
         format = Message.Format.MESSAGE_FORMAT)
   void errorRegisteringBackup();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212208, value =     "Less than {0}%\n{1}\nYou are in danger of running out of RAM. Have you set paging parameters " +
                                          "on your addresses? (See user manual \"Paging\" chapter)"  ,
         format = Message.Format.MESSAGE_FORMAT)
   void memoryError(Integer memoryWarningThreshold, String info);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212209, value = "Error completing callback on replication manager"  ,
         format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingCallbackOnReplicationManager(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212211, value = "unable to send notification when broadcast group is stopped" ,
         format = Message.Format.MESSAGE_FORMAT)
   void broadcastBridgeStoppedError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212212, value = "unable to send notification when broadcast group is stopped",
         format = Message.Format.MESSAGE_FORMAT)
   void notificationBridgeStoppedError(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212213, value = "Could not bind to {0} ({1} address); " +
         "make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6)." +
         "\nIgnoring discovery group-address, but this may lead to cross talking.",
         format = Message.Format.MESSAGE_FORMAT)
   void ioDiscoveryError(String hostAddress, String s);


   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212214, value = "Group Handler timed-out waiting for sendCondition",
         format = Message.Format.MESSAGE_FORMAT)
   void groupHandlerSendTimeout();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212215, value = "Compressed large message tried to read {0} bytes from stream {1}",
         format = Message.Format.MESSAGE_FORMAT)
   void compressedLargeMessageError(int length, int nReadBytes);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 212216, value = "Moving data directory {0} to {1}",
            format = Message.Format.MESSAGE_FORMAT)
   void backupMovingDataAway(String oldPath, String newPath);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214001, value = "Failed to call onMessage", format = Message.Format.MESSAGE_FORMAT)
   void onMessageError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214002, value = "Failure in initialisation", format = Message.Format.MESSAGE_FORMAT)
   void initializationError(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214003, value = "failed to cleanup session", format = Message.Format.MESSAGE_FORMAT)
   void failedToCleanupSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214004, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToExecuteListener(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214005, value = "Failed to handle failover", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandleFailover(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214006, value = "XA end operation failed ", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingEnd(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214007, value = "XA start operation failed {0} code:{1}", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingStart(String message, Integer code);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214008, value = "Session is not XA", format = Message.Format.MESSAGE_FORMAT)
   void sessionNotXA();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214009, value = "Received exception asynchronously from server", format = Message.Format.MESSAGE_FORMAT)
   void receivedExceptionAsynchronously(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214010, value = "Failed to handle packet", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandlePacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214011, value = "Failed to stop discovery group", format = Message.Format.MESSAGE_FORMAT)
   void failedToStopDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214012, value = "Failed to start discovery group", format = Message.Format.MESSAGE_FORMAT)
   void failedToStartDiscovery(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214013, value = "Failed to receive datagram", format = Message.Format.MESSAGE_FORMAT)
   void failedToReceiveDatagramInDiscovery(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214014, value = "Failed to call discovery listener", format = Message.Format.MESSAGE_FORMAT)
   void failedToCallListenerInDiscovery(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214015, value = "Error deploying URI {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeployingURI(@Cause Throwable e, URI uri);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214016, value = "Error deploying URI", format = Message.Format.MESSAGE_FORMAT)
   void errorDeployingURI(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214017, value = "Error undeploying URI {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorUnDeployingURI(@Cause Throwable e, URI a);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214018, value = "key attribute missing for configuration {0}", format = Message.Format.MESSAGE_FORMAT)
   void keyAttributeMissing(Node node);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214019, value = "Unable to deply node {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToDeployNode(@Cause Exception e, Node node);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214020, value = "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidFilter(@Cause Throwable t, SimpleString filter);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214021, value = "page subscription = {0} error={1}", format = Message.Format.MESSAGE_FORMAT)
   void pageSubscriptionError(IOAsyncTask ioAsyncTask, String error);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214022, value = "Failed to store id", format = Message.Format.MESSAGE_FORMAT)
   void batchingIdError(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214023, value = "Cannot find message {0}", format = Message.Format.MESSAGE_FORMAT)
   void cannotFindMessage(Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214024, value = "Cannot find queue messages for queueID={0} on ack for messageID={1}", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueue(Long queue, Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214025, value = "Cannot find queue messages {0} for message {1} while processing scheduled messages", format = Message.Format.MESSAGE_FORMAT)
   void journalCannotFindQueueScheduled(Long queue, Long id);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214026, value = "error releasing resources", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageErrorReleasingResources(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214027, value = "failed to expire messages for queue", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringMessages(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214028, value = "Failed to close session", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214029, value = "Caught XA exception", format = Message.Format.MESSAGE_FORMAT)
   void caughtXaException(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214030, value = "Caught exception", format = Message.Format.MESSAGE_FORMAT)
   void caughtException(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214031, value = "Invalid packet {0}", format = Message.Format.MESSAGE_FORMAT)
   void invalidPacket(Packet packet);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214032, value = "Failed to create session", format = Message.Format.MESSAGE_FORMAT)
   void failedToCreateSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214033, value = "Failed to reattach session", format = Message.Format.MESSAGE_FORMAT)
   void failedToReattachSession(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214034, value = "Failed to handle create queue", format = Message.Format.MESSAGE_FORMAT)
   void failedToHandleCreateQueue(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214035, value = "Unexpected error handling packet {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingPacket(@Cause Throwable t, Packet packet);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214036, value = "Failed to decode packet", format = Message.Format.MESSAGE_FORMAT)
   void errorDecodingPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214037, value = "Failed to execute failure listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingFailureListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214039, value = "Stomp Error, tx already exist! {0}", format = Message.Format.MESSAGE_FORMAT)
   void stompErrorTXExists(String txID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214040, value = "Error encoding stomp packet", format = Message.Format.MESSAGE_FORMAT)
   void errorEncodingStompPacket(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214041, value = "Cannot create stomp ping frame due to encoding problem.", format = Message.Format.MESSAGE_FORMAT)
   void errorOnStompPingFrame(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214042, value = "Failed to write to handler on invm connector {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorWritingToInvmConnector(@Cause Exception e, Runnable runnable);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214043, value = "error flushing invm channel", format = Message.Format.MESSAGE_FORMAT)
   void errorflushingInvmChannel(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214044, value = "Failed to execute connection life cycle listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingLifeCycleListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214045, value = "Failed to create netty connection", format = Message.Format.MESSAGE_FORMAT)
   void errorCreatingNettyConnection(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214046, value = "Failed to stop acceptor", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingAcceptor();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214047, value = "large message sync: largeMessage instance is incompatible with it, ignoring data", format = Message.Format.MESSAGE_FORMAT)
   void largeMessageIncomatible();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214049, value = "Failed to ack on Bridge", format = Message.Format.MESSAGE_FORMAT)
   void failedToAckOnBridge(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214050, value =  "-------------------------------Stomp begin tx: {0}", format = Message.Format.MESSAGE_FORMAT)
   void stompBeginTX(String txID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214052, value =  "Failed to stop bridge", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingBridge(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214053, value =  "Failed to pause bridge", format = Message.Format.MESSAGE_FORMAT)
   void errorPausingBridge(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214054, value =  "Failed to broadcast connector configs", format = Message.Format.MESSAGE_FORMAT)
   void errorBroadcastingConnectorConfigs(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214055, value =  "Failed to close consumer", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214056, value =  "Failed to close cluster connection flow record", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingFlowRecord(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214057, value =  "Failed to update cluster connection topology", format = Message.Format.MESSAGE_FORMAT)
   void errorUpdatingTopology(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214058, value =  "cluster connection Failed to handle message", format = Message.Format.MESSAGE_FORMAT)
   void errorHandlingMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214059, value =  "Failed to ack old reference", format = Message.Format.MESSAGE_FORMAT)
   void errorAckingOldReference(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214060, value =  "Failed to expire message reference", format = Message.Format.MESSAGE_FORMAT)
   void errorExpiringRef(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214061, value =  "Failed to remove consumer", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214062, value =  "Failed to deliver", format = Message.Format.MESSAGE_FORMAT)
   void errorDelivering(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214064, value =  "Failed to send forced delivery message", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingForcedDelivery(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214065, value =  "error acknowledging message", format = Message.Format.MESSAGE_FORMAT)
   void errorAckingMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214066, value =  "Failed to run large message deliverer", format = Message.Format.MESSAGE_FORMAT)
   void errorRunningLargeMessageDeliverer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214068, value =  "Failed to delete large message file", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingLargeMessageFile(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214069, value =  "Failed to remove temporary queue {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingTempQueue(@Cause Exception e, SimpleString bindingName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214070, value =  "Cannot find consumer with id {0}", format = Message.Format.MESSAGE_FORMAT)
   void cannotFindConsumer(long consumerID);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214072, value =  "Failed to call notification listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingNotifListener(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214073, value =  "Unable to call Hierarchical Repository Change Listener", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingRepoListener(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214074, value =  "failed to timeout transaction, xid:{0}", format = Message.Format.MESSAGE_FORMAT)
   void errorTimingOutTX(@Cause Exception e, Xid xid);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214075, value =  "Caught unexpected Throwable", format = Message.Format.MESSAGE_FORMAT)
   void caughtunexpectedThrowable(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214076, value =  "Failed to invoke getTextContent() on node {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransform(@Cause Throwable t, Node n);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214077, value =  "Invalid configuration", format = Message.Format.MESSAGE_FORMAT)
   void errorOnXMLTransformInvalidConf(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214078, value =  "exception while stopping the replication manager", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingReplicationManager(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214079, value = "Bridge Failed to ack", format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailedToAck(@Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214080, value = "Live server will not fail-back automatically", format = Message.Format.MESSAGE_FORMAT)
   void autoFailBackDenied();


   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214081, value = "Exception happened while stopping Discovery BroadcastEndpoint {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStoppingDiscoveryBroadcastEndpoint(Object endpoint, @Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 214082,
      value = "Backup server that requested fail-back was not announced. Server will not stop for fail-back.",
      format = Message.Format.MESSAGE_FORMAT)
   void failbackMissedBackupAnnouncement();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 214083,
      value = "Can't find queue {0} while reloading PAGE_CURSOR_COMPLETE, deleting record now",
      format = Message.Format.MESSAGE_FORMAT)
   void cantFindQueueOnPageComplete(long queueID);
}
