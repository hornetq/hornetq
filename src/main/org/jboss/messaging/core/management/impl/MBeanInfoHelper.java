/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;

import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MBeanInfoHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static MBeanOperationInfo[] getMBeanOperationsInfo(
         final Class mbeanInterface)
   {
      List<MBeanOperationInfo> operations = new ArrayList<MBeanOperationInfo>();

      for (Method method : mbeanInterface.getMethods())
      {
         if (!isGetterMethod(method) && !isSetterMethod(method)
               && !isIsBooleanMethod(method))
         {
            operations.add(getOperationInfo(method));
         }
      }

      return operations.toArray(new MBeanOperationInfo[0]);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static boolean isGetterMethod(final Method method)
   {
      if (!(method.getName().equals("get"))
            && method.getName().startsWith("get")
            && method.getParameterTypes().length == 0
            && !method.getReturnType().equals(void.class))
      {
         return true;
      }

      return false;
   }

   private static boolean isSetterMethod(final Method method)
   {
      if (!(method.getName().equals("set"))
            && method.getName().startsWith("set")
            && method.getParameterTypes().length == 1
            && method.getReturnType().equals(void.class))
      {
         return true;
      } else
      {
         return false;
      }
   }

   private static boolean isIsBooleanMethod(final Method method)
   {
      if (!(method.getName().equals("is")) && method.getName().startsWith("is")
            && method.getParameterTypes().length == 0
            && method.getReturnType().equals(boolean.class))
      {
         return true;
      } else
      {
         return false;
      }
   }

   private static MBeanOperationInfo getOperationInfo(final Method operation)
   {
      MBeanOperationInfo info = null;
      Class<?> returnType = operation.getReturnType();

      MBeanParameterInfo[] paramsInfo = getParametersInfo(operation
            .getParameterAnnotations(), operation.getParameterTypes());

      String description = operation.getName();
      int impact = MBeanOperationInfo.UNKNOWN;

      if (operation.getAnnotation(Operation.class) != null)
      {
         description = operation.getAnnotation(Operation.class).desc();
         impact = operation.getAnnotation(Operation.class).impact();
      }
      info = new MBeanOperationInfo(operation.getName(), description,
            paramsInfo, returnType.getName(), impact);

      return info;
   }

   private static MBeanParameterInfo[] getParametersInfo(
         final Annotation[][] params, final Class<?>[] paramTypes)
   {
      MBeanParameterInfo[] paramsInfo = new MBeanParameterInfo[params.length];

      for (int i = 0; i < params.length; i++)
      {
         MBeanParameterInfo paramInfo = null;
         String type = paramTypes[i].getName();
         for (Annotation anno : params[i])
         {
            if (Parameter.class.isInstance(anno))
            {
               String name = Parameter.class.cast(anno).name();
               String description = Parameter.class.cast(anno).desc();
               paramInfo = new MBeanParameterInfo(name, type, description);
            }
         }

         if (paramInfo == null)
         {
            paramInfo = new MBeanParameterInfo("p " + (i + 1), type,
                  "parameter " + (i + 1));
         }

         paramsInfo[i] = paramInfo;
      }

      return paramsInfo;
   }

   // Inner classes -------------------------------------------------
}
