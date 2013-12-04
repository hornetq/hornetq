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
package org.hornetq.core.protocol.stomp;

import org.hornetq.core.server.impl.ServerMessageImpl;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * Logger Code 33
 * <p>
 * Each message id must be 6 digits long starting with 10, the 3rd digit should be 9. So the range
 * is from 339000 to 339999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */

@MessageBundle(projectCode = "HQ")
public interface HornetQStompProtocolMessageBundle
{
   HornetQStompProtocolMessageBundle BUNDLE = Messages.getBundle(HornetQStompProtocolMessageBundle.class);

   @Message(id = 339000, value = "Stomp Connection TTL cannot be negative: {0}", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException negativeConnectionTTL(Long ttl);

   @Message(id = 339001, value = "Destination doesn't exist: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException destinationNotExist(String destination);

   @Message(id = 339002, value = "Stomp versions not supported: {0}", format = Message.Format.MESSAGE_FORMAT)   
   HornetQStompException versionNotSupported(String acceptVersion);

   @Message(id = 339003, value = "Header host is null", format = Message.Format.MESSAGE_FORMAT)   
   HornetQStompException nullHostHeader();

   @Message(id = 339004, value = "Cannot accept null as host", format = Message.Format.MESSAGE_FORMAT)
   String hostCannotBeNull();

   @Message(id = 339005, value = "Header host doesn't match server host", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException hostNotMatch();

   @Message(id = 339006, value = "host {0} doesn't match server host name", format = Message.Format.MESSAGE_FORMAT)
   String hostNotMatchDetails(String host);

   @Message(id = 339007, value = "Connection was destroyed.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException connectionDestroyed();

   @Message(id = 339008, value = "Connection hasn't been established.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException connectionNotEstablished();

   @Message(id = 339009, value = "Exception getting session", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorGetSession(@Cause Exception e);

   @Message(id = 339010, value = "Connection is not valid.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException invalidConnection();

   @Message(id = 339011, value = "Error sending message {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorSendMessage(ServerMessageImpl message, @Cause Exception e);

   @Message(id = 339012, value = "Error beginning a transaction {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorBeginTx(String txID, @Cause Exception e);

   @Message(id = 339013, value = "Error committing {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorCommitTx(String txID, @Cause Exception e);

   @Message(id = 339014, value = "Error aborting {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorAbortTx(String txID, @Cause Exception e);

   @Message(id = 339015, value = "Client must set destination or id header to a SUBSCRIBE command", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException noDestination();

   @Message(id = 339016, value = "Error creating subscription {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorCreatSubscription(String subscriptionID, @Cause Exception e);

   @Message(id = 339017, value = "Error unsubscribing {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorUnsubscrib(String subscriptionID, @Cause Exception e);

   @Message(id = 339018, value = "Error acknowledging message {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorAck(String messageID, @Cause Exception e);

   @Message(id = 339019, value = "Invalid char sequence: two consecutive CRs.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException invalidTwoCRs();

   @Message(id = 339020, value = "Invalid char sequence: There is a CR not followed by an LF", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException badCRs();

   @Message(id = 339021, value = "Expect new line char but is {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException notValidNewLine(byte b);

   @Message(id = 339022, value = "Expect new line char but is {0}", format = Message.Format.MESSAGE_FORMAT)
   String unexpectedNewLine(byte b);

   @Message(id = 339023, value = "Invalid STOMP frame: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException invalidCommand(String dumpByteArray);

   @Message(id = 339024, value = "Invalid STOMP frame: {0}", format = Message.Format.MESSAGE_FORMAT)
   String invalidFrame(String dumpByteArray);

   @Message(id = 339025, value = "failed to ack because no message with id: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException failToAckMissingID(long id);

   @Message(id = 339026, value = "subscription id {0} does not match {1}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException subscriptionIDMismatch(String subscriptionID, String actualID);

   @Message(id = 339027, value = "Cannot create a subscriber on the durable subscription if the client-id of the connection is not set", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException missingClientID();

   @Message(id = 339028, value = "Message header too big, increase minLargeMessageSize please.", format = Message.Format.MESSAGE_FORMAT)
   Exception headerTooBig();

   @Message(id = 339029, value = "Unsupported command: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException unknownCommand(String command);

   @Message(id = 339030, value = "transaction header is mandatory to COMMIT a transaction", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException needTxIDHeader();

   @Message(id = 339031, value = "Error handling send", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException errorHandleSend(@Cause Exception e);

   @Message(id = 339032, value = "Need a transaction id to begin", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException beginTxNoID();

   @Message(id = 339033, value = "transaction header is mandatory to ABORT a transaction", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException abortTxNoID();

   @Message(id = 339034, value = "This method should not be called", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException invalidCall();

   @Message(id = 339035, value = "Must specify the subscription's id or the destination you are unsubscribing from", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException needIDorDestination();

   @Message(id = 339036, value = "Encoding error.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException encodingError();

   @Message(id = 339037, value = "Must specify the subscription's id", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException needSubscriptionID();

   @Message(id = 339038, value = "Encoding error", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException encodingErrorWithCause(@Cause Exception e);

   @Message(id = 339039, value = "No id header in ACK/NACK frame.", format = Message.Format.MESSAGE_FORMAT)
   HornetQStompException noIDInAck();
}
