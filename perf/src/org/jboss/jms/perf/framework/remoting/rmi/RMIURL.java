/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.rmi;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class RMIURL
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int port;
   private String host;
   private String name;

   // Constructors --------------------------------------------------

   public RMIURL(String url) throws Exception
   {
      String s = url;
      if (s.startsWith("rmi://"))
      {
         s = s.substring(6);
         int idx = s.indexOf(':');
         if (idx != -1)
         {
            host = s.substring(0, idx);
            s = s.substring(idx + 1);
            idx = s.indexOf('/');
            if (idx != -1)
            {
               String sp = s.substring(0, idx);
               port = Integer.parseInt(sp);
               name = s.substring(idx + 1);
               return;
            }
         }
      }

      throw new Exception("Invalid RMI URL: " + url);

   }

   // Public --------------------------------------------------------

   public int getPort()
   {
      return port;
   }

   public String getHost()
   {
      return host;
   }

   public String getName()
   {
      return name;
   }

   public String getURL()
   {
      return "//" + (host != null ? host : "localhost") + ":" + port + "/" + name;
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
