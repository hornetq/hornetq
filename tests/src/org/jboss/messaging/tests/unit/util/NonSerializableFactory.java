/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.unit.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import org.jboss.util.naming.Util;

/**
 * used by the default context when running in embedded local configuration
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class NonSerializableFactory implements ObjectFactory
{

    public NonSerializableFactory()
    {
    }

    public static void unbind(Context ctx, String strName)
            throws NamingException
    {
        Name name = ctx.getNameParser("").parse(strName);
        int size = name.size();
        String atom = name.get(size - 1);
        Context parentCtx = Util.createSubcontext(ctx, name.getPrefix(size - 1));
        String key = (new StringBuilder()).append(parentCtx.getNameInNamespace()).append("/").append(atom).toString();
        getWrapperMap().remove(key);
        Util.unbind(ctx, strName);
    }


    public static void rebind(Context ctx, String strName, Object value)
            throws NamingException
    {
        Name name = ctx.getNameParser("").parse(strName);
        int size = name.size();
        String atom = name.get(size - 1);
        Context parentCtx = Util.createSubcontext(ctx, name.getPrefix(size - 1));
        String key = (new StringBuilder()).append(parentCtx.getNameInNamespace()).append("/").append(atom).toString();
        getWrapperMap().put(key, value);
        String className = value.getClass().getName();
        String factory = NonSerializableFactory.class.getName();
        StringRefAddr addr = new StringRefAddr("nns", key);
        Reference memoryRef = new Reference(className, addr, factory, null);
        parentCtx.rebind(atom, memoryRef);
    }

    public static void bind(Context ctx, String strName, Object value)
            throws NamingException
    {
        Name name = ctx.getNameParser("").parse(strName);
        int size = name.size();
        String atom = name.get(size - 1);
        Context parentCtx = Util.createSubcontext(ctx, name.getPrefix(size - 1));
        String key = (new StringBuilder()).append(parentCtx.getNameInNamespace()).append("/").append(atom).toString();
        getWrapperMap().put(key, value);
        String className = value.getClass().getName();
        String factory = NonSerializableFactory.class.getName();
        StringRefAddr addr = new StringRefAddr("nns", key);
        Reference memoryRef = new Reference(className, addr, factory, null);

        parentCtx.bind(atom, memoryRef);
    }

   public static Object lookup(String name)  throws NamingException
    {
        if(getWrapperMap().get(name) == null)
        {
           throw new NamingException(name + " not found");
        }
        return getWrapperMap().get(name);
    }

    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable env)
            throws Exception
    {
        Reference ref = (Reference) obj;
        RefAddr addr = ref.get("nns");
        String key = (String) addr.getContent();
        return getWrapperMap().get(key);
    }

   public static Map getWrapperMap()
   {
      return wrapperMap;
   }

    private static Map wrapperMap = Collections.synchronizedMap(new HashMap());
}