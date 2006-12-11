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

package org.jboss.test.messaging.core.ha;

import java.util.Properties;
import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import junit.framework.TestCase;
import org.jboss.logging.Logger;

/**
 * Define HOST1 and HOST2 as System variable (java -DHOST1=YourHost -DHOST2=YourHost2) pointing to a JBoss instance
 * with JBossMessaging installed.
 * This can usually be done using util scripts to deploy a JBoss instance.
 *
 * @author Clebert Suconic
 */
public abstract class HATestBase extends TestCase
{

    protected Logger log = Logger.getLogger(getClass());

    protected ConnectionFactory factoryServer1;
    protected ConnectionFactory factoryServer2;

    protected Context ctx1;
    protected Context ctx2;


    protected String NODE1 =System.getProperty("NODE1","localhost:1199");
    protected String NODE2 =System.getProperty("NODE2","localhost:1299");


   public void setUp(String jndiFactory) throws Exception
   {
      super.setUp();

      System.out.println("Server1=" + NODE1);
      System.out.println("Server2=" + NODE2);

      ctx1 = getContext(NODE1);
      ctx2 = getContext(NODE2);

      factoryServer1 = (ConnectionFactory) ctx1.lookup(jndiFactory);
      factoryServer2 = (ConnectionFactory) ctx2.lookup(jndiFactory);
   }

   public void setUp() throws Exception
   {
      this.setUp("/ConnectionFactory");
   }

   protected Context getContext(String host) throws Exception
    {
        // don't worry about this yet, we will put this in more generic way.
        // This is for test purposes only.
        String jndiProviderClass = "org.jnp.interfaces.NamingContextFactory";
        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY,
            jndiProviderClass);
        contextProperties.put(Context.PROVIDER_URL,
            "jnp://"+ host);
        return new InitialContext(contextProperties);

    }


    protected ConnectionFactory getFactoryServer1() {
        return factoryServer1;
    }

    protected ConnectionFactory getFactoryServer2() {
        return factoryServer2;
    }

    protected String getNODE1() {
        return NODE1;
    }

    protected String getNODE2() {
        return NODE2;
    }

    public Context getCtx1() {
        return ctx1;
    }

    public Context getCtx2() {
        return ctx2;
    }

}
