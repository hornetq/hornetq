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

import org.hornetq.api.core.HornetQAddressFullException;
import org.hornetq.api.core.HornetQConnectionTimedOutException;
import org.hornetq.api.core.HornetQDisconnectedException;
import org.hornetq.api.core.HornetQDuplicateMetaDataException;
import org.hornetq.api.core.HornetQIOErrorException;
import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQIncompatibleClientServerException;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.HornetQInvalidFilterExpressionException;
import org.hornetq.api.core.HornetQLargeMessageException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.HornetQObjectClosedException;
import org.hornetq.api.core.HornetQQueueExistsException;
import org.hornetq.api.core.HornetQSecurityException;
import org.hornetq.api.core.HornetQSessionCreationException;
import org.hornetq.api.core.HornetQTransactionOutcomeUnknownException;
import org.hornetq.api.core.HornetQTransactionRolledBackException;
import org.hornetq.api.core.HornetQUnBlockedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.security.CheckType;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Connection;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;
import org.w3c.dom.Node;

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
   HornetQInternalErrorException clientSessionClosed();

   @Message(id = 119007, value = "Failed to create session", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException failedToCreateSession(@Cause Throwable t);

   @Message(id = 119008, value = "Internal Error! ClientSessionFactoryImpl::createSessionInternal "
                                          + "just reached a condition that was not supposed to happen. "
                                      + "Please inform this condition to the HornetQ team", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException clietSessionInternal();

   @Message(id = 119009, value = "Queue can not be both durable and temporary", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException queueMisConfigured();

   @Message(id = 119010, value = "Interrupted awaiting completion in large message controller", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException largeMessageControllerInterrupted(@Cause Exception e);

   @Message(id = 119011, value = "Failed to initialise session factory", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException failedToInitialiseSessionFactory(@Cause Exception e);

   @Message(id = 119012, value = "connections for {0} closed by management" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException connectionsClosedByManagement(String ipAddress);

   @Message(id = 119013, value = "journals are not JournalImpl. You can't set a replicator!" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException notJournalImpl();

   @Message(id = 119014, value = "Unable to complete IO operation - {0}" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException unableToCompleteIOOperation(String message);

   @Message(id = 119015, value = "Exception in Netty transport" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException nettyError();

   @Message(id = 119016, value = "unhandled error during replication" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationUnhandledError(@Cause Exception e);

   @Message(id = 119017, value = "Live Node contains more journals than the backup node. Probably a version match error", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationTooManyJournals();

   @Message(id = 119018, value = "Unhandled file type {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationUnhandledFileType(ReplicationSyncFileMessage.FileType fileType);

   @Message(id = 119019, value = "Remote Backup can not be up-to-date!", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationBackupUpToDate();

   @Message(id = 119020, value = "unhandled data type!", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationUnhandledDataType();

   @Message(id = 119021, value = "No binding for divert {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException noBindingForDivert(SimpleString name);

   @Message(id = 119022, value = "Binding {0} is not a divert", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException bindingNotDivert(SimpleString name);

   @Message(id = 119023, value = "Error trying to start replication", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException replicationStartError(@Cause Exception e);

   @Message(id = 119024, value = "error trying to stop {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException errorStoppingServer(@Cause Exception e, HornetQServer server);

   @Message(id = 119025, value =  "Channel disconnected", format = Message.Format.MESSAGE_FORMAT)
   HornetQNotConnectedException channelDisconnected();

   @Message(id = 119026, value =  "Cannot connect to server(s). Tried with all available servers.", format = Message.Format.MESSAGE_FORMAT)
   HornetQNotConnectedException cannotConnectToServers();

   @Message(id = 119027, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   HornetQNotConnectedException cannotConnectToStaticConnectors(@Cause Exception e);

   @Message(id = 119028, value =  "Failed to connect to any static connectors", format = Message.Format.MESSAGE_FORMAT)
   HornetQNotConnectedException cannotConnectToStaticConnectors2();

   @Message(id = 119029, value =  "Connection is destroyed", format = Message.Format.MESSAGE_FORMAT)
   HornetQNotConnectedException connectionDestroyed();

   @Message(id = 119030, value =  "Did not receive data from server for {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQConnectionTimedOutException connectionTimedOut(Connection transportConnection);

   @Message(id = 119031, value =  "Timed out waiting to receive initial broadcast from cluster", format = Message.Format.MESSAGE_FORMAT)
   HornetQConnectionTimedOutException connectionTimedOutInInitialBroadcast();

   @Message(id = 119032, value =  "Timed out waiting to receive cluster topology. Group:{0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQConnectionTimedOutException connectionTimedOutOnReceiveTopology(DiscoveryGroup discoveryGroup);

   @Message(id = 119033, value =  "Timed out waiting for response when sending packet {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQConnectionTimedOutException timedOutSendingPacket(Byte type);

   @Message(id = 119034, value =  "Did not receive data from {0}. It is likely the client has exited or crashed without " +
                                  "closing its connection, or the network between the server and client has failed. " +
                                  "You also might have configured connection-ttl and client-failure-check-period incorrectly. " +
                                  "Please check user manual for more information." +
                                  " The connection will now be closed.", format = Message.Format.MESSAGE_FORMAT)
   HornetQConnectionTimedOutException clientExited(String remoteAddress);

   @Message(id = 119035, value =  "The connection was disconnected because of server shutdown", format = Message.Format.MESSAGE_FORMAT)
   HornetQDisconnectedException disconnected();

   @Message(id = 119036, value =  "Connection failure detected. Unblocking a blocking call that will never get a response", format = Message.Format.MESSAGE_FORMAT)
   HornetQUnBlockedException unblockingACall();

   @Message(id = 119037, value =  "Timeout on waiting I/O completion", format = Message.Format.MESSAGE_FORMAT)
   HornetQIOErrorException ioTimeout();

   @Message(id = 119038, value =  "queue {0} has been removed cannot deliver message, queues should not be removed when grouping is used", format = Message.Format.MESSAGE_FORMAT)
   HornetQNonExistentQueueException groupingQueueRemoved(SimpleString chosenClusterName);

   @Message(id = 119039, value =  "Queue {0}does not exist", format = Message.Format.MESSAGE_FORMAT)
   HornetQNonExistentQueueException noSuchQueue(SimpleString queueName);

   @Message(id = 119040, value =  "Binding already exists {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQQueueExistsException bindingAlreadyExists(Binding binding);

   @Message(id = 119041, value =  "Queue already exists {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQQueueExistsException queueAlreadyExists(SimpleString queueName);

   @Message(id = 119042, value =  "Consumer is closed", format = Message.Format.MESSAGE_FORMAT)
   HornetQObjectClosedException consumerClosed();

   @Message(id = 119043, value =  "Producer is closed", format = Message.Format.MESSAGE_FORMAT)
   HornetQObjectClosedException producerClosed();

   @Message(id = 119044, value =  "Session is closed", format = Message.Format.MESSAGE_FORMAT)
   HornetQObjectClosedException sessionClosed();

   @Message(id = 119045, value =  "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);

   @Message(id = 119046, value =  "Cannot call receive(...) - a MessageHandler is set", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException messageHandlerSet();

   @Message(id = 119047, value =  "Cannot set MessageHandler - consumer is in receive(...)", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException inReceive();

   @Message(id = 119048, value =  "Header size ({0}) is too big, use the messageBody for large data, or increase minLargeMessageSize",
         format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException headerSizeTooBig(Integer headerSize);

   @Message(id = 119049, value =  "The large message lost connection with its session, either because of a rollback or a closed session", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException largeMessageLostSession();

   @Message(id = 119050, value =  "Couldn't select a TransportConfiguration to create SessionFactory", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException noTCForSessionFactory();

   @Message(id = 119051, value =  "MessageId was not assigned to Message", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException messageIdNotAssigned();

   @Message(id = 119052, value =  "Cannot compare journals if not in sync!", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException journalsNotInSync();

   @Message(id = 119053, value =  "Connected server is not a backup server", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException serverNotBackupServer();

   @Message(id = 119054, value =  "Backup replication server is already connected to another server", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException alreadyHaveReplicationServer();

   @Message(id = 119055, value =  "Cannot delete queue {0} on binding {1} - it has consumers = {2}", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException cannotDeleteQueue(SimpleString name, SimpleString queueName, String s);

   @Message(id = 119056, value =  "Backup Server was not yet in sync with live", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException backupServerNotInSync();

   @Message(id = 119057, value =  "Could not find reference on consumer ID={0}, messageId = {1} queue = {2}", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException consumerNoReference(Long id, Long messageID, SimpleString name);

   @Message(id = 119058, value =  "Consumer {0} doesn't exist on the server" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException consumerDoesntExist(long consumerID);

   @Message(id = 119059, value =  "No address configured on the Server's Session" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException noAddress();

   @Message(id = 119060, value =  "large-message not initialized on server", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException largeMessageNotInitialised();

   @Message(id = 119061, value =  "Unable to validate user: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQSecurityException unableToValidateUser(String user);

   @Message(id = 119062, value =  "User: {0} doesn't have permission='{1}' on address {2}", format = Message.Format.MESSAGE_FORMAT)
   HornetQSecurityException userNoPermissions(String username, CheckType checkType, String saddress);

   @Message(id = 119063, value =  "Server and client versions incompatible", format = Message.Format.MESSAGE_FORMAT)
   HornetQIncompatibleClientServerException incompatibleCLientServer();

   @Message(id = 119064, value =  "Error saving the message body", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException errorSavingBody(@Cause Exception e);

   @Message(id = 119065, value =  "Error reading the LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException errorReadingBody(@Cause Exception e);

   @Message(id = 119066, value =  "Error closing stream from LargeMessageBody", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException errorClosingLargeMessage(@Cause Exception e);

   @Message(id = 119067, value =  "Timeout waiting for LargeMessage Body", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException timeoutOnLargeMessage();

   @Message(id = 119068, value =  "Error on saving Large Message Buffer", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException errorSavingLargeMessageBuffer(@Cause Exception handledException);

   @Message(id = 119069, value =  "Error writing body of message", format = Message.Format.MESSAGE_FORMAT)
   HornetQLargeMessageException errorWritingLargeMessage(@Cause Exception e);

   @Message(id = 119070, value =  "The transaction was rolled back on failover to a backup server", format = Message.Format.MESSAGE_FORMAT)
   HornetQTransactionRolledBackException txRolledBack();

   @Message(id = 119071, value =  "Server not started", format = Message.Format.MESSAGE_FORMAT)
   HornetQSessionCreationException serverNotStarted();

   @Message(id = 119072, value =  "Metadata {0}={1} had been set already", format = Message.Format.MESSAGE_FORMAT)
   HornetQDuplicateMetaDataException duplicateMetadata(String key, String data);

   @Message(id = 119073, value =  "The transaction was rolled back on failover however commit may have been succesful", format = Message.Format.MESSAGE_FORMAT)
   HornetQTransactionOutcomeUnknownException txOutcomeUnknown();

   @Message(id = 119074, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidType(Object type);

   @Message(id = 119075, value = "Invalid type: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidEncodeType(Object type);

   @Message(id = 119076, value = "Params for management operations must be of the following type: int long double String boolean Map or array thereof but found {0}",
         format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidManagementParam(Object type);

   @Message(id = 119077, value = "Invalid window size {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidWindowSize(Integer size);

   @Message(id = 119078, value = "retry interval must be positive, was {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidRetryInterval(Long size);

   @Message(id = 119079, value = "{0} must neither be null nor empty", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException emptyOrNull(String name);

   @Message(id = 119080, value = "{0}  must be greater than 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZero(String name, Number val);

   @Message(id = 119081, value = "{0} must be a valid percentual value between 0 and 100 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException notPercent(String name, Number val);

   @Message(id = 119082, value = "{0}  must be equals to -1 or greater than 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanMinusOne(String name, Number val);

   @Message(id = 119083, value = "{0}  must be equals to -1 or greater or equals to 0 (actual value: {1})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZeroOrMinusOne(String name, Number val);

   @Message(id = 119084, value = "{0} must be betwen {1} and {2} inclusive (actual value: {3})", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustbeBetween(String name, Integer minPriority, Integer maxPriority, Object value);

   @Message(id = 119085, value = "Invalid journal type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidJournalType(String val);

   @Message(id = 119086, value = "Invalid address full message policy type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidAddressFullPolicyType(String val);

   @Message(id = 119087, value = "No operation mapped to int {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noOperationMapped(Integer operation);

   @Message(id = 119088, value = "invalid value: {0} count must be greater than 0", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException greaterThanZero(Integer count);

   @Message(id = 119089, value = "Cannot set Message Counter Sample Period < {0}ms", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidMessageCounterPeriod(Long period);

   @Message(id = 119090, value = "invalid new Priority value: {0}. It must be between 0 and 9 (both included)", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidNewPriority(Integer period);

   @Message(id = 119091, value = "No queue found for {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noQueueFound(String otherQueueName);

   @Message(id = 119092, value = "Only NIO and AsyncIO are supported journals", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidJournal();

   @Message(id = 119093, value = "Invalid journal type {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidJournalType2(JournalType journalType);

   @Message(id = 119094, value = "Directory {0} does not exist and cannot be created", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException cannotCreateDir(String dir);

   @Message(id = 119095, value = "Invalid index {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidIndex(Integer index);

   @Message(id = 119096, value = "Cannot convert to int", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException cannotConvertToInt();

   @Message(id = 119097, value = "Routing name is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException routeNameIsNull();

   @Message(id = 119098, value = "Cluster name is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException clusterNameIsNull();

   @Message(id = 119099, value = "Address is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException addressIsNull();

   @Message(id = 119100, value = "Binding type not specified", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException bindingTypeNotSpecified();

   @Message(id = 119101, value = "Binding ID is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException bindingIdNotSpecified();

   @Message(id = 119102, value = "Distance is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException distancenotSpecified();

   @Message(id = 119103, value = "Invalid last Received Command ID: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidCommandID(Integer lastReceivedCommandID);

   @Message(id = 119104, value = "Cannot find channel with id {0} to close", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noChannelToClose(Long id);

   @Message(id = 119105, value = "Close Listener cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException closeListenerCannotBeNull();

   @Message(id = 119106, value = "Fail Listener cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failListenerCannotBeNull();

   @Message(id = 119107, value = "Connection already exists with id {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException connectionExists(Object id);

   @Message(id = 119108, value = "Acceptor with id {0} already registered", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException acceptorExists(Integer id);

   @Message(id = 119109, value = "Acceptor with id {0} not registered", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException acceptorNotExists(Integer id);

   @Message(id = 119110, value = "Invalid argument null listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullListener();

   @Message(id = 119111, value = "Invalid argument null handler", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullHandler();

   @Message(id = 119112, value = "Unknown protocol {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException unknownProtocol(ProtocolType protocol);

   @Message(id = 119113, value = "node id is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeIdNull();

   @Message(id = 119114, value = "Cannot have a replicated backup without configuring its live-server!", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noLiveForReplicatedBackup();

   @Message(id = 119115, value = "Queue name is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException queueNameIsNull();

   @Message(id = 119116, value = "Cannot find resource with name {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException cannotFindResource(String resourceName);

   @Message(id = 119117, value = "no getter method for {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noGetterMethod(String resourceName);

   @Message(id = 119118, value = "no operation {0}/{1}", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noOperation(String operation, Integer length);

   @Message(id = 119119, value = "match can not be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullMatch();

   @Message(id = 119120, value = "* can only be at end of match", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException invalidMatch();

   @Message(id = 119121, value = "User cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullUser();

   @Message(id = 119122, value = "Password cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullPassword();

   @Message(id = 119123, value = "No available codec to decode password!", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noCodec();

   @Message(id = 119124, value = "the first node to be compared is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException firstNodeNull();

   @Message(id = 119125, value = "the second node to be compared is null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException secondNodeNull();

   @Message(id = 119126, value = "nodes have different node names", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHaveDifferentNames();

   @Message(id = 119127, value = "nodes hava a different number of attributes", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHaveDifferentAttNumber();

   @Message(id = 119128, value = "attribute {0}={1} doesn't match", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException attsDontMatch(String name, String value);

   @Message(id = 119129, value = "one node has children and the other doesn't" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException oneNodeHasChildren();

   @Message(id = 119130, value = "nodes hava a different number of children" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nodeHasDifferentChildNumber();

   @Message(id = 119131, value = "Element {0} requires a valid Boolean value, but '{1}' cannot be parsed as a Boolean" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeBoolean(Node elem, String value);

   @Message(id = 119132, value = "Element {0} requires a valid Double value, but '{1}' cannot be parsed as a Double" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeDouble(Node elem, String value);

   @Message(id = 119133, value = "Element {0} requires a valid Integer value, but '{1}' cannot be parsed as a Integer" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeInteger(Node elem, String value);

   @Message(id = 119134, value = "Element {0} requires a valid Long value, but '{1}' cannot be parsed as a Long" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException mustBeLong(Node elem, String value);

   @Message(id = 119135, value = "Failed to get decoder" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException failedToGetDecoder(@Cause Exception e);

   @Message(id = 119136, value = "Error decoding password" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errordecodingPassword(@Cause Exception e);

   @Message(id = 119137, value = "Error instantiating transformer class {0}" , format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException errorCreatingTransformerClass(@Cause Exception e, String transformerClassName);

   @Message(id = 119138, value = "method autoEncode doesn't know how to convert {0} yet", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException autoConvertError(Class<? extends Object> aClass);

   @Message(id = 119139, value = "Address \"{0}\" is full. Message encode size = {1}B", format = Message.Format.MESSAGE_FORMAT)
   HornetQAddressFullException addressIsFull(String addressName, int size);

   /** Message used on on {@link org.hornetq.core.server.impl.HornetQServerImpl#destroyConnectionWithSessionMetadata(String, String)} */
   @Message(id = 119140, value = "Executing destroyConnection with {0}={1} through management's request", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataHeader(String key, String value);

   /** Message used on on {@link org.hornetq.core.server.impl.HornetQServerImpl#destroyConnectionWithSessionMetadata(String, String)} */
   @Message(id = 119141, value = "Closing connection {0}", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataClosingConnection(String serverSessionString);

   /** Exception used on on {@link org.hornetq.core.server.impl.HornetQServerImpl#destroyConnectionWithSessionMetadata(String, String)} */
   @Message(id = 119142, value = "Disconnected per admin's request on {0}={1}", format = Message.Format.MESSAGE_FORMAT)
   HornetQDisconnectedException destroyConnectionWithSessionMetadataSendException(String key, String value);

   /** Message used on on {@link org.hornetq.core.server.impl.HornetQServerImpl#destroyConnectionWithSessionMetadata(String, String)} */
   @Message(id = 119143, value = "No session found with {0}={1}", format = Message.Format.MESSAGE_FORMAT)
   String destroyConnectionWithSessionMetadataNoSessionFound(String key, String value);

}
