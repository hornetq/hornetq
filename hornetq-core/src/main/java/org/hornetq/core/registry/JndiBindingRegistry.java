package org.hornetq.core.registry;

import org.hornetq.spi.core.naming.BindingRegistry;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class JndiBindingRegistry implements BindingRegistry
{
   private Context context;

   /**
    * @return the context
    */
   public Object getContext()
   {
      return context;
   }

   /**
    * @param context the context to set
    */
   public void setContext(Object context)
   {
      this.context = (Context)context;
   }

   public JndiBindingRegistry(Context context)
   {
      this.context = context;
   }

   public JndiBindingRegistry() throws Exception
   {
      this.context = new InitialContext();
   }

   public Object lookup(String name)
   {
      try
      {
         if (context == null)
         {
            return null;
         }
         else
         {
            return context.lookup(name);
         }
      }
      catch (NamingException e)
      {
         return null;
      }
   }

   public boolean bind(String name, Object obj)
   {
      try
      {
         return bindToJndi(name, obj);
      }
      catch (NamingException e)
      {
         throw new RuntimeException(e);
      }
   }

   public void unbind(String name)
   {
      try
      {
         if (context != null)
         {
            context.unbind(name);
         }
      }
      catch (NamingException e)
      {
      }
   }

   public void close()
   {
      try
      {
         if (context != null)
         {
            context.close();
         }
      }
      catch (NamingException e)
      {
      }
   }


   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      if (context != null)
      {
         String parentContext;
         String jndiNameInContext;
         int sepIndex = jndiName.lastIndexOf('/');
         if (sepIndex == -1)
         {
            parentContext = "";
         }
         else
         {
            parentContext = jndiName.substring(0, sepIndex);
         }
         jndiNameInContext = jndiName.substring(sepIndex + 1);
         try
         {
            context.lookup(jndiName);

            //JMSServerManagerImpl.log.warn("Binding for " + jndiName + " already exists");
            return false;
         }
         catch (Throwable e)
         {
            // OK
         }

         Context c = org.hornetq.utils.JNDIUtil.createContext(context, parentContext);

         c.rebind(jndiNameInContext, objectToBind);
      }
      return true;
   }
}
