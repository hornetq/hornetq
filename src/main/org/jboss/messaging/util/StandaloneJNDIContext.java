/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;
import org.jboss.messaging.jms.client.colocated.ColocatedConnectionDelegateFactory;
import org.jboss.messaging.jms.server.standard.StandardMessageBroker;

/**
 * A Context that can be used to locally lookup ConnectionFactories and Destinations. It gets
 * the mapping from the JNDI environment (jndi.properties).
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class StandaloneJNDIContext implements Context
{

//   private static final Logger log = Logger.getLogger(StandaloneJNDIContext.class);

   /**
    * @param environment - The possibly null JNDI environment, as received by the
    *        InitialContextFactory.
    **/
   StandaloneJNDIContext(Hashtable environment)
   {
   }


   public Object lookup(String name) throws NamingException
   {

      if ("ConnectionFactory".equals(name))
      {
         // TODO review this code, this is not the right way of getting a connection factory
         ColocatedConnectionDelegateFactory cdFactory =
               new ColocatedConnectionDelegateFactory(new StandardMessageBroker());

         return new JBossConnectionFactory(cdFactory);
      }

      throw new NameNotFoundException(name + " not found");
   }

   public Object lookup(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }


   public void bind(Name name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void bind(String name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rebind(Name name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rebind(String name, Object obj) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void unbind(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void unbind(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rename(Name oldName, Name newName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void rename(String oldName, String newName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration list(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration list(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration listBindings(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NamingEnumeration listBindings(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void destroySubcontext(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void destroySubcontext(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Context createSubcontext(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Context createSubcontext(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object lookupLink(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object lookupLink(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NameParser getNameParser(Name name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public NameParser getNameParser(String name) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Name composeName(Name name, Name prefix) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public String composeName(String name, String prefix) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object addToEnvironment(String propName, Object propVal) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Object removeFromEnvironment(String propName) throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public Hashtable getEnvironment() throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public void close() throws NamingException
   {
      throw new NotYetImplementedException();
   }

   public String getNameInNamespace() throws NamingException
   {
      throw new NotYetImplementedException();
   }

}
