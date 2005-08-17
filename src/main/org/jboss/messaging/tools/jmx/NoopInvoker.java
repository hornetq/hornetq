/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jmx;

import org.jboss.invocation.Invoker;
import org.jboss.invocation.Invocation;

import java.io.Serializable;

public class NoopInvoker implements Serializable, Invoker
{
   public String getServerHostName() throws Exception
   {
      return "localhost";
   }


   public Object invoke(Invocation invocation) throws Exception
   {
      return null;
   }
}
