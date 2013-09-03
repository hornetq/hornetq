package org.hornetq.jms.server;


import org.hornetq.api.core.HornetQAddressExistsException;
import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQInternalErrorException;
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
public interface HornetQJMSServerBundle
{
   HornetQJMSServerBundle BUNDLE = Messages.getBundle(HornetQJMSServerBundle.class);

   @Message(id = 129000, value =  "Connection Factory {0} does not exist" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException cfDoesntExist(String name);

   @Message(id = 129001, value =  "Invalid signature {0} parsing Connection Factory" , format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException invalidSignatureParsingCF(String sig);

   @Message(id = 129002, value = "Invalid node {0} parsing Connection Factory", format = Message.Format.MESSAGE_FORMAT)
   HornetQInternalErrorException invalidNodeParsingCF(String name);

   @Message(id = 129003, value = "Discovery Group ''{0}'' does not exist on main config", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException discoveryGroupDoesntExist(String name);

   @Message(id = 129004, value = "No Connector name configured on create ConnectionFactory", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException noConnectorNameOnCF();

   @Message(id = 129005, value = "Connector ''{0}'' not found on the main configuration file" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException noConnectorNameConfiguredOnCF(String name);

   @Message(id = 129006, value =  "JNDI {0} is already being used by another connection factory", format = Message.Format.MESSAGE_FORMAT)
   HornetQAddressExistsException cfJndiExists(String name);

   @Message(id = 129007, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException errorDecodingPassword(@Cause Exception e);
}
