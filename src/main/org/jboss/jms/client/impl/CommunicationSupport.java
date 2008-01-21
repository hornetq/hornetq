/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.jms.client.impl;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import javax.jms.JMSException;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.util.Streamable;
import org.jgroups.persistence.CannotConnectException;
import org.jboss.messaging.util.Version;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * // TODO find a better name for this class
 */
public abstract class CommunicationSupport <T extends CommunicationSupport<?>> implements Streamable, Serializable 
{
   private static final Logger log = Logger.getLogger(CommunicationSupport.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   // This is set on the server.
   protected String id;
   
   
   transient private boolean firstTime = true;
   
   
   // getVersion cached (instead of calling it every time)
   transient private byte cacheVersion;

   public CommunicationSupport(String id)
   {
      super();
      this.id = id;
   }

   public CommunicationSupport()
   {
      this("NO_ID_SET");
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
   
   // Fields ---------------------------------------------------------------------------------------

   protected abstract Client getClient();
   
   protected byte getVersion()
   {
      return Version.instance().getProviderIncrementingVersion();
   }
   
   public String getID()
   {
      return id;
   }

   public void setId(String id)
   {
      this.id = id;
   }
   
   // Protected Methods-----------------------------------------------------------------------------
   
   protected void sendOneWay(AbstractPacket packet) throws JMSException
   {
      sendOneWay(getClient(), id, lookupVersion(), packet);
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
   
   
   protected  AbstractPacket sendBlocking(AbstractPacket request) throws JMSException
   {
      return sendBlocking(getClient(), id, lookupVersion(), request);
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
   
   private byte lookupVersion()
   {
      if (firstTime)
      {
         firstTime=false;
         cacheVersion = getVersion();
      }
      return cacheVersion;
   }
   
   protected static JMSException handleThrowable(Throwable t)
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

   

}
