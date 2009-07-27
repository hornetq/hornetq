/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.jboss.javaee.example.server;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.transaction.UserTransaction;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@MessageDriven(name = "MDB_BMTExample",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/testQueue"),
                        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Dups-ok-acknowledge")
                     })
@TransactionManagement(value= TransactionManagementType.BEAN)
public class MDB_BMTExample implements MessageListener
{
   @Resource
   MessageDrivenContext ctx;

   public void onMessage(Message message)
   {
      try
      {
         //Step 9. We know the client is sending a text message so we cast
         TextMessage textMessage = (TextMessage)message;

         //Step 10. get the text from the message.
         String text = textMessage.getText();

         System.out.println("message " + text + " received");

         //Step 11. lets look at the user transaction to make sure there isn't one.
         UserTransaction tx = ctx.getUserTransaction();

         if(tx != null)
         {
            tx.begin();
            System.out.println("we're in the middle of a transaction: " + tx);
            tx.commit();
         }
         else
         {
            System.out.println("something is wrong, I was expecting a transaction");
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}