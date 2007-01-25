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
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CreateHangingConsumerCommand implements Command
{
   private static final long serialVersionUID = -997724797145152821L;
   
   private ConnectionFactory cf;
   private Queue queue;

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
      
      MessageConsumer cons = sess.createConsumer(queue);

      cons.setMessageListener(new Listener());
      
      // leave the connection unclosed
      
      // return the remoting client session id for the connection
      return ((JBossConnection)conn).getRemotingClientSessionID();      
   }
   
   class Listener implements MessageListener
   {
      public void onMessage(Message m)
      {
      }
   }

}
