/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.example.jms.stateless.client;

import javax.naming.InitialContext;
import org.jboss.example.jms.common.ExampleSupport;
import org.jboss.example.jms.stateless.bean.StatelessSessionExample;
import org.jboss.example.jms.stateless.bean.StatelessSessionExampleHome;

/**
 * This example deploys a simple Stateless Session Bean that is used as a proxy to send and receive
 * JMS messages in a managed environment.
 *
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Client extends ExampleSupport
{
   public void example() throws Exception
   {            
      InitialContext ic = new InitialContext();
                  
      StatelessSessionExampleHome home =
         (StatelessSessionExampleHome)ic.lookup("ejb/StatelessSessionExample");            
      
      StatelessSessionExample bean = home.create();
                  
      String queueName = getDestinationJNDIName();
      String text = "Hello!";                  
      
      bean.drain(queueName);
                       
      bean.send("Hello!", queueName);
      log("The " + text + " message was successfully sent to the " + queueName + " queue");
          
      int num = bean.browse(queueName);
                  
      assertEquals(1, num);
          
      log("Queue browse result: " + num);            
      
      String result = bean.receive(queueName);
      log("Received " + result);            
      
      assertEquals("Hello!", result);            
      
      bean.remove();
   }
   
   protected boolean isQueueExample()
   {
      return true;
   }
   
   public static void main(String[] args)
   {
      new Client().run();
   }   
}
