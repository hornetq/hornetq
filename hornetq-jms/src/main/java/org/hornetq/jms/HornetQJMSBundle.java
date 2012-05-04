package org.hornetq.jms;


import org.hornetq.api.core.*;
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
public interface HornetQJMSBundle
{
   HornetQJMSBundle BUNDLE = Messages.getBundle(HornetQJMSBundle.class);

   @Message(id = 129001, value =  "Connection Factory {0} doesn't exist" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException cfDoesntExist(String name);

   @Message(id = 129002, value =  "Invalid signature {0} parsing Connection Factory" , format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException invalidSignatureParsingCF(String sig);

   @Message(id = 129003, value = "Invalid node {0} parsing Connection Factory", format = Message.Format.MESSAGE_FORMAT)
   InternalErrorException invalidNodeParsingCF(String name);

   @Message(id = 129004, value = "Discovery Group '{0}' doesn't exist on maing config", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException discoveryGroupDoesntExist(String name);

   @Message(id = 129005, value = "No Connector name configured on create ConnectionFactory", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException noConnectorNameOnCF();

   @Message(id = 129006, value = "Connector '{0}' not found on the main configuration file" , format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException noConnectorNameConfiguredOnCF(String name);

   @Message(id = 129007, value =  "JNDI {0} is already being used by another connection factory", format = Message.Format.MESSAGE_FORMAT)
   AddressExistsException cfJndiExists(String name);

   @Message(id = 129008, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException errorDecodingPassword(@Cause Exception e);
}
