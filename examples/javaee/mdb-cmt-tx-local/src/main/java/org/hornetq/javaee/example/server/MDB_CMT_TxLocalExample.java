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
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jboss.ejb3.annotation.ResourceAdapter;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageDriven(name = "MDB_CMT_TxLocalExample", activationConfig = { @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                                                                    @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
                                                                    @ActivationConfigProperty(propertyName = "useLocalTx", propertyValue = "true") })
@TransactionManagement(value = TransactionManagementType.CONTAINER)
@TransactionAttribute(value = TransactionAttributeType.NOT_SUPPORTED)
@ResourceAdapter("hornetq-ra.rar")
public class MDB_CMT_TxLocalExample implements MessageListener
{
   @Resource(mappedName = "java:/TransactionManager")
   private TransactionManager tm;

   public void onMessage(final Message message)
   {
      try
      {
         // Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         // Step 10. get the text from the message.
         String text = textMessage.getText();

         System.out.println("message " + text + " received");

         if (!textMessage.getJMSRedelivered())
         {
            // Step 11. On first delivery get the transaction, take a look, and throw an exception
            Transaction tx = tm.getTransaction();

            if (tx != null)
            {
               System.out.println("something is wrong, there should be no global transaction: " + tx);
            }
            else
            {
               System.out.println("there is no global transaction, altho the messge delivery is using a local transaction");
               System.out.println("lets throw an exception and see what happens");
               throw new RuntimeException("DOH!");
            }
         }
         else
         {
            // Step 12. Print the message
            System.out.println("The message was redelivered since the message delivery used a local transaction");
         }

      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
      catch (SystemException e)
      {
         e.printStackTrace();
      }
   }
}