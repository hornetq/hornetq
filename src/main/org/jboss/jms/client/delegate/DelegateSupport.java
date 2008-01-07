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
package org.jboss.jms.client.delegate;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import javax.jms.JMSException;

import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.Closeable;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.util.Streamable;
import org.jgroups.persistence.CannotConnectException;

/**
 * Base class for all client-side delegate classes.
 *
 * Client-side delegate classes provide an empty implementation of the appropriate delegate
 * interface. The classes are advised using JBoss AOP to provide the client side advice stack.
 * The methods in the delegate class will never actually be invoked since they will either be
 * handled in the advice stack or invoked on the server before reaching the delegate.
 *
 * The delegates are created on the server and serialized back to the client. When they arrive on
 * the client, the init() method is called which causes the advices to be bound to the advised
 * class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DelegateSupport<T extends HierarchicalState> implements Streamable, Serializable
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = -1362079381836473747L;

	private static final Logger log = Logger.getLogger(DelegateSupport.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   // This is set on the server.
   protected String id;

   // This is set on the client.
   // The reason we don't use the meta-data to store the state for the delegate is to avoid the
   // extra HashMap lookup that would entail. This can be significant since the state could be
   // queried for many aspects in an a single invocation.
   protected transient T state;

   protected transient byte version;

   protected transient Client client;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DelegateSupport(String id)
   {
      this.id = id;
      this.state = null;
   }

   public DelegateSupport()
   {
      this("NO_ID_SET");
   }

   // Interceptor implementation -------------------------------------------------------------------

   public String getName()
   {
      // Neede a meaninful name to change the aop stack programatically (HA uses that)
      return this.getClass().getName();
   }

   // Streamable implementation --------------------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      id = in.readUTF();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(id);
   }

   // Public ---------------------------------------------------------------------------------------

   public T getState()
   {
      return state;
   }

   public void setState(T state)
   {
      this.state = state;

      this.version = state.getVersionToUse().getProviderIncrementingVersion();
   }

   public String getID()
   {
      return id;
   }

   /**
    * During HA events, delegates corresponding to new enpoints on the new server are created and
    * the state of those delegates has to be transfered to the "failed" delegates. For example, a
    * "failed" connection delegate will have to assume the ID of the new connection endpoint, the
    * new RemotingConnection instance, etc.
    */
   public void synchronizeWith(DelegateSupport<T> newDelegate) throws Exception
   {
      id = newDelegate.getID();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void sendOneWay(AbstractPacket packet) throws JMSException
   {
      sendOneWay(client, id, version, packet);
   }

   protected static void sendOneWay(Client client, String targetID, byte version, AbstractPacket packet) throws JMSException
   {
      assert client != null;
      assertValidID(targetID);
      assert packet != null;

      packet.setVersion(version);
      packet.setTargetID(targetID);

      client.sendOneWay(packet);
   }

   protected AbstractPacket sendBlocking(AbstractPacket request) throws JMSException
   {
      return sendBlocking(client, id, version, request);
   }

   protected static AbstractPacket sendBlocking(Client client, String targetID, byte version, AbstractPacket request) throws JMSException
   {
      assert client != null;
      assertValidID(targetID);
      assert request != null;

      request.setVersion(version);
      request.setTargetID(targetID);
      try
      {
         AbstractPacket response = (AbstractPacket) client.sendBlocking(request);
         if (response instanceof JMSExceptionMessage)
         {
            JMSExceptionMessage message = (JMSExceptionMessage) response;
            throw message.getException();
         } else {
            return response;
         }
      } catch (Throwable t)
      {
         throw handleThrowable(t);
      }
   }

   protected void closeChildren()
   {
         Set<HierarchicalState> clone;

         Set<HierarchicalState> children = state.getChildren();

         if (children == null)
         {
            if (trace) { log.trace(this + " has no children"); }
            return;
         }

         synchronized (children)
         {
            clone = new HashSet<HierarchicalState>(children);
         }

         // Cycle through the children this will do a depth first close
         for (HierarchicalState child: clone)
         {
            Closeable del = (Closeable)child.getCloseableDelegate();
            try
            {
               del.closing(-1);
               del.close();
            }
            catch (Throwable t)
            {
               //We swallow exceptions in close/closing, this is because if the connection fails, it is naturally for code to then close
               //in a finally block, it would not then be appropriate to throw an exception. This is a common technique
               if (trace)
               {
                  log.trace("Failed to close", t);
               }
            }
         }
   }

   // Private --------------------------------------------------------------------------------------

   public static JMSException handleThrowable(Throwable t)
   {
      // ConnectionFailedException could happen during ConnectionFactory.createConnection.
      // IOException could happen during an interrupted exception.
      // CannotConnectionException could happen during a communication error between a connected
      // remoting client and the server (what means any new invocation).

      if (t instanceof JMSException)
      {
         return (JMSException)t;
      }
      else if ((t instanceof IOException))
      {
         return new MessagingNetworkFailureException((Exception)t);
      }
      //This can occur if failure happens when Client.connect() is called
      //Ideally remoting should have a consistent API
      else if (t instanceof RuntimeException)
      {
         RuntimeException re = (RuntimeException)t;

         Throwable initCause = re.getCause();

         if (initCause != null)
         {
            do
            {
               if ((initCause instanceof CannotConnectException) ||
                        (initCause instanceof IOException))
               {
                  return new MessagingNetworkFailureException((Exception)initCause);
               }
               initCause = initCause.getCause();
            }
            while (initCause != null);
         }
      }

      return new MessagingJMSException("Failed to invoke", t);
   }

   // Inner classes --------------------------------------------------------------------------------
}
