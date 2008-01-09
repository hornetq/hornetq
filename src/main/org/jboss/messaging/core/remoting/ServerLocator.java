/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

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
   private Map<String, String> parameters = new HashMap<String, String>();

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
      
      String query = u.getQuery();
      if (query != null)
      {
         StringTokenizer tok = new StringTokenizer(query, "&");
         while(tok.hasMoreTokens())
         {
            String token = tok.nextToken();
            int eq = token.indexOf("=");
            String name = (eq > -1) ? token.substring(0, eq) : token;
            String value = (eq > -1) ? token.substring(eq + 1) : "";
            parameters.put(name, value);
         }
      }
   }
  
   public ServerLocator(TransportType transport, String host, int port)
   {
      this(transport, host, port, new HashMap<String, String>());
   }
   
   public ServerLocator(TransportType transport, String host, int port, Map<String, String> parameters)
   {
      assert transport != null;
      assert host != null;
      assert port > 0;
      assert parameters != null;
      
      this.transport = transport;
      this.host = host;
      this.port = port;
      this.parameters = parameters;
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
   
   public Map<String, String> getParameters()
   {
      return parameters;
   }

   public String getURI()
   {
      String uri = transport + "://" + host + ":" + port + ((parameters.size() != 0) ? "?" : "");
      if(parameters.size() != 0)
      {
         Iterator<Entry<String, String>> iter = parameters.entrySet().iterator();
         while(iter.hasNext())
         {
            Entry<String, String> entry = iter.next();
            uri += entry.getKey() + "=" + entry.getValue();
            if(iter.hasNext())
            {
               uri += "&";
            }
         }
      }
      return uri;
   }

   @Override
   public String toString()
   {
      return "ServerLocator[uri=" + getURI() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
