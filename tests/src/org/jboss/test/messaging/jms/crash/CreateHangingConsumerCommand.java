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
package org.jboss.test.messaging.jms.crash;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;

import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.tools.jmx.rmi.Command;

/**
 * 
 * A CreateHangingConsumerCommand.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * CreateHangingConsumerCommand.java,v 1.1 2006/02/21 07:44:02 timfox Exp
 */
public class CreateHangingConsumerCommand implements Command
{
   private static final long serialVersionUID = -997724797145152821L;
   
   private ConnectionFactory cf;
   
   private Queue queue;
   
   private static MessageConsumer consumer;
   
   public CreateHangingConsumerCommand(ConnectionFactory cf, Queue queue)
   {
      this.cf = cf;
      
      this.queue = queue;
   }
   
   public Object execute() throws Exception
   {
      Connection conn = cf.createConnection();
          
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
       
      conn.start();
      
      consumer = sess.createConsumer(queue);
      
      consumer.setMessageListener(new Listener());
      
      //Leave the connection unclosed
      
      //Return the remoting client session id for the connection
      return ((JBossConnection)conn).getRemotingClientSessionId();      
   }
   
   class Listener implements MessageListener
   {
      public void onMessage(Message m)
      {
         
      }
   }

}
