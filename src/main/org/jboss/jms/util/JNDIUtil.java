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
import javax.naming.NamingEnumeration;
import javax.naming.Binding;
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


   public static void tearDownRecursively(Context c) throws Exception
   {
      for(NamingEnumeration ne = c.listBindings(""); ne.hasMore(); )
      {
         Binding b = (Binding)ne.next();
         String name = b.getName();
         Object object = b.getObject();
         if (object instanceof Context)
         {
            tearDownRecursively((Context)object);
         }
         c.unbind(name);
      }
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
