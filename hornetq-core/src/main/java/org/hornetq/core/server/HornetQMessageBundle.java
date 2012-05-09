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
package org.hornetq.core.server;

import org.hornetq.api.core.*;
import org.hornetq.api.core.SecurityException;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.security.CheckType;
import org.hornetq.spi.core.remoting.Connection;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 11
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 119000 to 119999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQMessageBundle
{
   HornetQMessageBundle BUNDLE = Messages.getBundle(HornetQMessageBundle.class);

   @Message(id = 119001, value = "Activation for server {0}", format = Message.Format.MESSAGE_FORMAT)
   String activationForServer(HornetQServer server);

   @Message(id = 119002, value = "Generating thread dump because - {0}", format = Message.Format.MESSAGE_FORMAT)
   String generatingThreadDump(String reason);

   @Message(id = 119003, value = "Thread {0} name = {1} id = {2} group = {3}", format = Message.Format.MESSAGE_FORMAT)
   String threadDump(Thread key, String name, Long id, ThreadGroup threadGroup);

   @Message(id = 119004, value = "End Thread dump", format = Message.Format.MESSAGE_FORMAT)
   String endThreadDump();

   @Message(id = 119005, value = "Information about server {0}\nCluster Connection:{1}", format = Message.Format.MESSAGE_FORMAT)
   String serverDescribe(String identity, String describe);

   @Message(id = 119006, value = "ClientSession closed while creating session", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException clientSessionClosed();

   @Message(id = 119007, value = "Failed to create session", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException failedToCreateSession(@Cause Throwable t);

   @Message(id = 119008, value = "Internal Error! ClientSessionFactoryImpl::createSessionInternal "
                                          + "just reached a condition that was not supposed to happen. "
                                      + "Please inform this condition to the HornetQ team", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException clietSessionInternal();

   @Message(id = 119009, value = "Queue can not be both durable and temporary", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException queueMisConfigured();

   @Message(id = 119010, value = "Interrupted awaiting completion in large message controller", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException largeMessageControllerInterrupted(@Cause Exception e);

   @Message(id = 119011, value = "Failed to initialise session factory", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException failedToInitialiseSessionFactory(@Cause Exception e);

   @Message(id = 119012, value = "connections for {0} closed by management" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException connectionsClosedByManagement(String ipAddress);

   @Message(id = 119013, value = "journals are not JournalImpl. You can't set a replicator!" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException notJournalImpl();

   @Message(id = 119014, value = "Unable to complete IO operation - {0}" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException unableToCompleteIOOperation(String message);

   @Message(id = 119015, value = "Exception in Netty transport" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException nettyError();

   @Message(id = 119016, value = "unhandled error during replication" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationUnhandledError(@Cause Exception e);

   @Message(id = 119017, value = "Live Node contains more journals than the backup node. Probably a version match error", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationTooManyJournals();

   @Message(id = 119018, value = "Unhandled file type {0}", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationUnhandledFileType(ReplicationSyncFileMessage.FileType fileType);

   @Message(id = 119019, value = "Remote Backup can not be up-to-date!", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationBackupUpToDate();

   @Message(id = 119020, value = "unhandled data type!", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationUnhandledDataType();

   @Message(id = 119021, value = "No binding for divert {0}", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException noBindingForDivert(SimpleString name);

   @Message(id = 119022, value = "Binding {0} is not a divert", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException bindingNotDivert(SimpleString name);

   @Message(id = 119023, value = "Error trying to start replication", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException replicationStartError(@Cause Exception e);

   @Message(id = 119024, value = "error trying to stop {0}", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException errorStoppingServer(@Cause Exception e, HornetQServer server);

   @Message(id = 119025, value =  "Channel disconnected", format = Message.Format.MESSAGE_FORMAT)
   NotConnectedException channelDisconnected();

   @Message(id = 119026, value =  "Cannot connect to server(s). Tried with all available servers.", format = Message.Format.MESSAGE_FORMAT)
   NotConnectedException cannotConnectToServers();

   @Message(id = 119027, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   NotConnectedException cannotConnectToStaticConnectors(@Cause Exception e);

   @Message(id = 119028, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   NotConnectedException cannotConnectToStaticConnectors2();

   @Message(id = 119029, value =  "Connection is destroyed", format = Message.Format.MESSAGE_FORMAT)
   NotConnectedException connectionDestroyed();

   @Message(id = 119030, value =  "Did not receive data from server for {0}", format = Message.Format.MESSAGE_FORMAT)
   ConnectionTimedOutException connectionTimedOut(Connection transportConnection);

   @Message(id = 119031, value =  "Timed out waiting to receive initial broadcast from cluster", format = Message.Format.MESSAGE_FORMAT)
   ConnectionTimedOutException connectionTimedOutInInitialBroadcast();

   @Message(id = 119032, value =  "Timed out waiting to receive cluster topology. Group:{0}", format = Message.Format.MESSAGE_FORMAT)
   ConnectionTimedOutException connectionTimedOutOnReceiveTopology(DiscoveryGroup discoveryGroup);

   @Message(id = 119033, value =  "Timed out waiting for response when sending packet {0}", format = Message.Format.MESSAGE_FORMAT)
   ConnectionTimedOutException timedOutSendingPacket(Byte type);

   @Message(id = 119034, value =  "Did not receive data from {0}. It is likely the client has exited or crashed without " +
                                  "closing its connection, or the network between the server and client has failed. " +
                                  "You also might have configured connection-ttl and client-failure-check-period incorrectly. " +
                                  "Please check user manual for more information." +
                                  " The connection will now be closed.", format = Message.Format.MESSAGE_FORMAT)
   ConnectionTimedOutException clientExited(String remoteAddress);

   @Message(id = 119035, value =  "The connection was disconnected because of server shutdown", format = Message.Format.MESSAGE_FORMAT)
   DisconnectedException disconnected();

   @Message(id = 119036, value =  "Connection failure detected. Unblocking a blocking call that will never get a response", format = Message.Format.MESSAGE_FORMAT)
   UnBlockedException unblockingACall();

   @Message(id = 119037, value =  "Timeout on waiting I/O completion", format = Message.Format.MESSAGE_FORMAT)
   IOErrorException ioTimeout();

   @Message(id = 119038, value =  "queue {0} has been removed cannot deliver message, queues should not be removed when grouping is used", format = Message.Format.MESSAGE_FORMAT)
   NonExistentQueueException groupingQueueRemoved(SimpleString chosenClusterName);

   @Message(id = 119039, value =  "Queue {0}does not exist", format = Message.Format.MESSAGE_FORMAT)
   NonExistentQueueException noSuchQueue(SimpleString queueName);

   @Message(id = 119040, value =  "Binding already exists {0}", format = Message.Format.MESSAGE_FORMAT)
   QueueExistsException bindingAlreadyExists(Binding binding);

   @Message(id = 119041, value =  "Queue already exists {0}", format = Message.Format.MESSAGE_FORMAT)
   QueueExistsException queueAlreadyExists(SimpleString queueName);

   @Message(id = 119042, value =  "Consumer is closed", format = Message.Format.MESSAGE_FORMAT)
   ObjectClosedException consumerClosed();

   @Message(id = 119043, value =  "Producer is closed", format = Message.Format.MESSAGE_FORMAT)
   ObjectClosedException producerClosed();

   @Message(id = 119044, value =  "Session is closed", format = Message.Format.MESSAGE_FORMAT)
   ObjectClosedException sessionClosed();

   @Message(id = 119045, value =  "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   InvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);

   @Message(id = 119046, value =  "Cannot call receive(...) - a MessageHandler is set", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException messageHandlerSet();

   @Message(id = 119047, value =  "Cannot set MessageHandler - consumer is in receive(...)", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException inReceive();

   @Message(id = 119048, value =  "Header size ({0}) is too big, use the messageBody for large data, or increase minLargeMessageSize",
         format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException headerSizeTooBig(Integer headerSize);

   @Message(id = 119049, value =  "The large message lost connection with its session, either because of a rollback or a closed session", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException largeMessageLostSession();

   @Message(id = 119050, value =  "Couldn't select a TransportConfiguration to create SessionFactory", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException noTCForSessionFactory();

   @Message(id = 119051, value =  "MessageId was not assigned to Message", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException messageIdNotAssigned();

   @Message(id = 119052, value =  "Cannot compare journals if not in sync!", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException journalsNotInSync();

   @Message(id = 119053, value =  "Connected server is not a backup server", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException serverNotBackupServer();

   @Message(id = 119054, value =  "Backup replication server is already connected to another server", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException alreadyHaveReplicationServer();

   @Message(id = 119055, value =  "Cannot delete queue {0} on binding {1} - it has consumers = {2}", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException cannotDeleteQueue(SimpleString name, SimpleString queueName, String s);

   @Message(id = 119056, value =  "Backup Server was not yet in sync with live", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException backupServerNotInSync();

   @Message(id = 119057, value =  "Could not find reference on consumer ID={0}, messageId = {1} queue = {2}", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException consumerNoReference(Long id, Long messageID, SimpleString name);

   @Message(id = 119058, value =  "Consumer {0} doesn't exist on the server" , format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException consumerDoesntExist(long consumerID);

   @Message(id = 119059, value =  "No address configured on the Server's Session" , format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException noAddress();

   @Message(id = 119060, value =  "large-message not initialized on server", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException largeMessageNotInitialised();

   @Message(id = 119061, value =  "Unable to validate user: {0}", format = Message.Format.MESSAGE_FORMAT)
   SecurityException unableToValidateUser(String user);

   @Message(id = 119062, value =  "User: {0} doesn't have permission='{1}' on address {2}", format = Message.Format.MESSAGE_FORMAT)
   SecurityException userNoPermissions(String username, CheckType checkType, String saddress);

   @Message(id = 119063, value =  "Server and client versions incompatible", format = Message.Format.MESSAGE_FORMAT)
   IncompatibleClientServerException incompatibleCLientServer();

   @Message(id = 119064, value =  "Error saving the message body", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException errorSavingBody(@Cause Exception e);

   @Message(id = 119065, value =  "Error reading the LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException errorReadingBody(@Cause Exception e);

   @Message(id = 119066, value =  "Error closing stream from LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException errorClosingLargeMessage(@Cause Exception e);

   @Message(id = 119067, value =  "Timeout waiting for LargeMessage Body", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException timeoutOnLargeMessage();

   @Message(id = 119068, value =  "Error on saving Large Message Buffer", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException errorSavingLargeMessageBuffer(@Cause Exception handledException);

   @Message(id = 119069, value =  "Error writing body of message", format = Message.Format.MESSAGE_FORMAT)
   LargeMessageException errorWritingLargeMessage(@Cause Exception e);

   @Message(id = 119070, value =  "The transaction was rolled back on failover to a backup server", format = Message.Format.MESSAGE_FORMAT)
   TransactionRolledBackException txRolledBack();

   @Message(id = 119071, value =  "Server not started", format = Message.Format.MESSAGE_FORMAT)
   SessionCreationException serverNotStarted();

   @Message(id = 119072, value =  "Metadata {0}={1} had been set already", format = Message.Format.MESSAGE_FORMAT)
   DuplicateMetaDataException duplicateMetadata(String key, String data);

   @Message(id = 119073, value =  "The transaction was rolled back on failover however commit may have been succesful", format = Message.Format.MESSAGE_FORMAT)
   TransactionOutcomeUnknownException txOutcomeUnknown();


}
