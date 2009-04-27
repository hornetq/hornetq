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

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.jboss.ejb3.annotation.ResourceAdapter;


/**
 * MDB that is connected to the remote queue, using an alternate resource-adapter
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */

//Step 10. The message is received on the MDB, using a remote queue and a different ResourceAdapter
@MessageDriven(name = "MDB_QueueB",
               activationConfig =
                     {
                        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "queue/B"),
                        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge")
                    })
              @ResourceAdapter("example-jbm-ra.rar")

public class MDBQueueB implements MessageListener
{
   public void onMessage(Message message)
   {
      try
      {
         TextMessage tm = (TextMessage)message;

         String text = tm.getText();


         System.out.println("Step 10: (MDBQueueB.java) Message received using the default adapter. Message = \"" + text + "\"" );
         
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
