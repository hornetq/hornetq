package org.hornetq.integration.aerogear;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 23
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 239000 to 239999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQAeroGearBundle
{
   HornetQAeroGearBundle BUNDLE = Messages.getBundle(HornetQAeroGearBundle.class);

   @Message(id = 239000, value =  "endpoint can not be null" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException endpointNull();

   @Message(id = 239001, value =  "application-id can not be null" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException applicationIdNull();

   @Message(id = 239002, value =  "master-secret can not be null" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException masterSecretNull();

   @Message(id = 239003, value =  "{0}: queue {1} not found" , format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException noQueue(String connectorName, String queueName);
}
