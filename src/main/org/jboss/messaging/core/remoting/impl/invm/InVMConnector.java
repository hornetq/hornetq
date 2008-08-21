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
package org.jboss.messaging.core.remoting.impl.invm;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;

/**
 * A InVMConnector
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMConnector implements Connector
{
   private final int id;
   
   private final RemotingHandler handler;
   
   private final ConnectionLifeCycleListener listener;
   
   private final InVMAcceptor acceptor;
   
   public InVMConnector(final int id, final RemotingHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      this.id = id;
      
      this.handler = handler;   
      
      this.listener = listener;
      
      InVMRegistry registry = InVMRegistry.instance;
      
      registry.registerConnector(id, this);
      
      acceptor = registry.getAcceptor(id);
      
      if (acceptor == null)
      {
         throw new IllegalStateException("Cannot connect to invm acceptor with id " + id + " has it been started?");
      }
   }

   public void close()
   {      
      InVMRegistry.instance.unregisterConnector(id);            
   }

   public Connection createConnection()
   {
      Connection conn = new InVMConnection(acceptor.getHandler(), new Listener());
      
      acceptor.connect((String)conn.getID(), handler);
           
      return conn;
   }

   public void start()
   {          
   }
   
   public RemotingHandler getHandler()
   {
      return handler;
   }
   
   private class Listener implements ConnectionLifeCycleListener
   {
      public void connectionCreated(Connection connection)
      {
         listener.connectionCreated(connection);
      }

      public void connectionDestroyed(Object connectionID)
      {
         //Close the correspond connection on the other side
         acceptor.disconnect((String)connectionID);
         
         listener.connectionDestroyed(connectionID);
      }

      public void connectionException(Object connectionID, MessagingException me)
      {
         listener.connectionException(connectionID, me);
      }
      
   }

}
