/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ServerLocator
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private TransportType transport;
   private String host;
   private int port;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ServerLocator(String uri) throws URISyntaxException
   {
      assert uri != null;
      assert uri.length() > 0;

      URI u = new URI(uri);

      try
      {
         String scheme = u.getScheme().toUpperCase();
         this.transport = TransportType.valueOf(scheme);
      } catch (IllegalArgumentException e)
      {
         URISyntaxException use = new URISyntaxException(uri, u.getScheme()
               .toUpperCase()
               + " transport type is not supported");
         use.initCause(e);
         
         throw use;
      }
      this.host = u.getHost();
      this.port = u.getPort();
   }

   public ServerLocator(TransportType transport, String host, int port)
   {
      this.transport = transport;
      this.host = host;
      this.port = port;
   }

   // Public --------------------------------------------------------

   public TransportType getTransport()
   {
      return transport;
   }

   public String getHost()
   {
      return host;
   }

   public int getPort()
   {
      return port;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((host == null) ? 0 : host.hashCode());
      result = prime * result + port;
      result = prime * result
            + ((transport == null) ? 0 : transport.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      final ServerLocator other = (ServerLocator) obj;
      if (host == null)
      {
         if (other.host != null)
            return false;
      } else if (!host.equals(other.host))
         return false;
      if (port != other.port)
         return false;
      if (transport == null)
      {
         if (other.transport != null)
            return false;
      } else if (!transport.equals(other.transport))
         return false;
      return true;
   }

   public String getURI()
   {
      return transport + "://" + host + ":" + port;
   }

   @Override
   public String toString()
   {
      return "RemoteServiceLocator[uri=" + getURI() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
