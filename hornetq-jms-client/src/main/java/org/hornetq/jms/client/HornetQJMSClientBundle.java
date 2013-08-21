package org.hornetq.jms.client;


import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQInvalidFilterExpressionException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.SimpleString;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 129000 to 129999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQJMSClientBundle
{
   HornetQJMSClientBundle BUNDLE = Messages.getBundle(HornetQJMSClientBundle.class);

   @Message(id = 129000, value =  "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);

   @Message(id = 129001, value =  "Invalid Subscription Name. It is required to set the subscription name", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException invalidSubscriptionName();

   @Message(id = 129002, value =  "Destination {0} does not exist", format = Message.Format.MESSAGE_FORMAT)
   HornetQNonExistentQueueException destinationDoesNotExist(SimpleString destination);

   @Message(id = 129003, value =  "name cannot be null", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nameCannotBeNull();

   @Message(id = 129004, value =  "name cannot be empty", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nameCannotBeEmpty();

   @Message(id = 129005, value =  "It is illegal to call this method from within a Message Listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateRuntimeException callingMethodFromListenerRuntime();

   @Message(id = 129006, value =  "It is illegal to call this method from within a Message Listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException callingMethodFromListener();

   @Message(id = 129007, value =  "It is illegal to call this method from within a Completion Listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateRuntimeException callingMethodFromCompletionListenerRuntime();

   @Message(id = 129008, value =  "It is illegal to call this method from within a Completion Listener", format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException callingMethodFromCompletionListener();

   @Message(id = 129009, value = "Null {0} is not allowed", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException nullArgumentNotAllowed(String type);

   @Message(id = 129010, value = "Topic (Destination) cannot be null", format = Message.Format.MESSAGE_FORMAT)
   InvalidDestinationException nullTopic();

   @Message(id = 129011, value = "LargeMessage streaming is only possible on ByteMessage or StreamMessage",
            format = Message.Format.MESSAGE_FORMAT)
   IllegalStateException onlyValidForByteOrStreamMessages();

   @Message(id = 129012, value = "The property name \'{0}\' is not a valid java identifier.",
            format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidJavaIdentifier(String propertyName);

   @Message(id = 129013, value = "Message is read-only", format = Message.Format.MESSAGE_FORMAT)
   MessageNotWriteableException messageNotWritable();

   @Message(id = 129014, value = "Message is write-only", format = Message.Format.MESSAGE_FORMAT)
   MessageNotReadableException messageNotReadable();

   @Message(id = 129015, value = "Illegal deliveryMode value: {0}", format = Message.Format.MESSAGE_FORMAT)
   JMSException illegalDeliveryMode(int deliveryMode);
}
