/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.javaee.example.server;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.jboss.ejb3.annotation.ResourceAdapter;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageDriven(name = "MDB_CMT_SetRollbackOnlyExample", activationConfig = { @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                                                                            @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue") })
@TransactionManagement(value = TransactionManagementType.CONTAINER)
@TransactionAttribute(value = TransactionAttributeType.REQUIRED)
@ResourceAdapter("hornetq-ra.rar")
public class MDB_CMT_SetRollbackOnlyExample implements MessageListener
{
   @Resource
   MessageDrivenContext ctx;

   public void onMessage(final Message message)
   {
      try
      {
         // Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         // Step 10. get the text from the message.
         String text = textMessage.getText();

         if (!textMessage.getJMSRedelivered())
         {
            // Step 11. rollback delivery of message if the first time
            System.out.println("message " + text + " received for the first time");
            ctx.setRollbackOnly();
         }
         else
         {
            // Step 12. read the message
            System.out.println("message " + text + " received for the second time");
         }

      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }
}