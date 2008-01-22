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
package org.jboss.messaging.microcontainer;

import java.io.Serializable;
import java.util.Hashtable;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.util.naming.Util;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JndiBinder
{

   private String bindTo;
   private Object target;
   private Hashtable properties;

   public JndiBinder()
   {
   }


   public void setBindTo(String bindTo)
   {
      this.bindTo = bindTo;
   }


   public void setTarget(Object target)
   {
      this.target = target;
   }


   public void setJndiProperties(Hashtable properties)
   {
      this.properties = properties;
   }

   public void start()
           throws Exception
   {
      InitialContext ctx = getInitialContext(properties);

      try
      {
         if (target.getClass().isAssignableFrom(Serializable.class))
         {
            Util.rebind(ctx, bindTo, target);
         }
         else
         {
            bindToNonSerializableFactory(ctx);
         }

      }
      catch (NamingException e)
      {

         e.printStackTrace();
         NamingException namingException = new NamingException((new StringBuilder()).append("Could not bind JndiBinder service into JNDI under jndiName:").append(ctx.getNameInNamespace()).append("/").append(bindTo).toString());
         namingException.setRootCause(e);
         throw namingException;
      }
   }



   public void stop()
           throws Exception
   {
      InitialContext ctx = getInitialContext(properties);

      if (target.getClass().isAssignableFrom(Serializable.class))
      {
         Util.unbind(ctx, bindTo);
      }
      else
      {
         unbindFromSerializableFactory(ctx);
      }

   }

   private void unbindFromSerializableFactory(InitialContext ctx)
           throws NamingException
   {
      NonSerializableFactory.unbind(ctx, bindTo);
   }

   protected void bindToNonSerializableFactory(InitialContext ctx)
           throws NamingException
   {
      NonSerializableFactory.bind(ctx, bindTo, target);
   }

   private static InitialContext getInitialContext(Hashtable props)
           throws NamingException
   {
      InitialContext ctx = null;
      if (props != null)

         ctx = new InitialContext(props);

      else ctx = new InitialContext();
      return ctx;
   }
}