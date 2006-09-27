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

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Topic;

import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.tools.jmx.rmi.Command;

/**
 * 
 * A CreateClientOnServerCommand.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version 1.1
 *
 * $Id$
 */
public class CreateTwoClientOnServerCommand implements Command
{
   private static final long serialVersionUID = -997724797145152821L;
   
   private ConnectionFactory cf;
   
   private boolean retainReference;
   
   private static List commands = new ArrayList();
   
   Topic topic;
   Connection conn1;
   Connection conn2;
   
   public CreateTwoClientOnServerCommand(ConnectionFactory cf,  Topic topic, boolean retainReference)
   {
      this.cf = cf;
      this.topic = topic;
      
      this.retainReference = retainReference;
   }
   
   /*
    * Just create a connection, send and receive a message and leave the connection open.
    */
   public Object execute() throws Exception
   {
      if (retainReference)
      {
         commands.add(this);
      }
      
      conn1 = cf.createConnection();
      conn1.setClientID("test1");
      conn1.start();

      conn2 = cf.createConnection();
      conn2.setClientID("test2");
      conn2.start();

      conn1.close();
      
      String arrays[] = new String[2];
      arrays[0] = ((JBossConnection)conn1).getRemotingClientSessionId();
      arrays[1] = ((JBossConnection)conn2).getRemotingClientSessionId();

      // Return the remoting client session id for the connection
      return arrays;      
   }

}
