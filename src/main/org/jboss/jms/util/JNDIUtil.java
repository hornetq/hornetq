/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.util;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.util.StringTokenizer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDIUtil
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * Create a context path recursively.
    */
   public static Context createContext(Context c, String path) throws NamingException
   {
      Context crtContext = c;
      for(StringTokenizer st = new StringTokenizer(path, "/"); st.hasMoreTokens(); )
      {
         String tok = st.nextToken();

         try
         {
            Object o = crtContext.lookup(tok);
            if (!(o instanceof Context))
            {
               throw new NamingException("Path " + path + " overwrites and already bound object");
            }
            crtContext = (Context)o;
            continue;
         }
         catch(NameNotFoundException e)
         {
            // OK
         }
         crtContext = crtContext.createSubcontext(tok);
      }
      return crtContext;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
