package org.hornetq.core.protocol.proton;

import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPIllegalStateException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPInternalErrorException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPInvalidFieldException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPNotImplementedException;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * Logger Code 11
 * <p>
 * Each message id must be 6 digits long starting with 10, the 3rd digit should be 9. So the range
 * is from 219000 to 119999.
 * <p>
 * Once released, methods should not be deleted as they may be referenced by knowledge base
 * articles. Unused methods should be marked as deprecated.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQAMQPProtocolMessageBundle
{
   HornetQAMQPProtocolMessageBundle BUNDLE = Messages.getBundle(HornetQAMQPProtocolMessageBundle.class);


   @Message(id = 219000, value =  "target address not set", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInvalidFieldException targetAddressNotSet();

   @Message(id = 219001, value =  "error creating temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInternalErrorException errorCreatingTemporaryQueue(String message);

   @Message(id = 219002, value =  "target address does not exist", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException addressDoesntExist();

   @Message(id = 219003, value =  "error finding temporary queue, {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInternalErrorException errorFindingTemporaryQueue(String message);

   @Message(id = 219004, value =  "error creating HornetQ Session, {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInternalErrorException errorCreatingHornetQSession(String message);

   @Message(id = 219005, value =  "error creating HornetQ Consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInternalErrorException errorCreatingHornetQConsumer(String message);

   @Message(id = 219006, value =  "error starting HornetQ Consumer, {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorStartingConsumer(String message);

   @Message(id = 219007, value =  "error acknowledging message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorAcknowledgingMessage(long messageID, String message);

   @Message(id = 219008, value =  "error cancelling message {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorCancellingMessage(long messageID, String message);

   @Message(id = 219009, value =  "error closing consumer {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorClosingConsumer(long consumerID, String message);

   @Message(id = 219010, value =  "source address does not exist", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInvalidFieldException sourceAddressDoesntExist();

   @Message(id = 219011, value =  "source address not set", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPInvalidFieldException sourceAddressNotSet();

   @Message(id = 219012, value =  "error rolling back coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorRollingbackCoordinator(String message);

   @Message(id = 219013, value =  "error committing coordinator: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPIllegalStateException errorCommittingCoordinator(String message);

   @Message(id = 219014, value =  "not implemented: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQAMQPNotImplementedException notImplemented(String message);

   @Message(id = 219015, value =  "error decoding AMQP frame", format = Message.Format.MESSAGE_FORMAT)
   String decodeError();
}
