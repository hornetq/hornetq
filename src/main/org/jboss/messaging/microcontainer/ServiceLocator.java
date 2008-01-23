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

import org.jboss.dependency.spi.ControllerContext;
import org.jboss.kernel.spi.dependency.KernelControllerContext;
import org.jboss.kernel.spi.dependency.KernelControllerContextAware;
import org.jboss.security.AuthenticationManager;
import org.jboss.tm.TransactionManagerLocator;

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

/**
 * This is a layer that is used for injecting services into other objects. depending on the configuration we are running
 * we can get these from more than one place or even inject them.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServiceLocator implements KernelControllerContextAware
{
    private ObjectName multiplexer;
    private TransactionManager tm;
    private org.jboss.security.AuthenticationManager authenticationManager;
    private DataSource dataSource;
    private KernelControllerContext kernelControllerContext;

    public void setKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
    {
        this.kernelControllerContext = kernelControllerContext;
    }

    public void unsetKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
    {
        kernelControllerContext = null;
    }

    public TransactionManager getTransactionManager() throws Exception
    {
        if(tm == null)
        {
            ControllerContext controllerContext = kernelControllerContext.getController().getInstalledContext("jbm:TransactionManager");
           if(controllerContext != null)
           {
              tm = (TransactionManager) controllerContext.getTarget();
           }
           else
           {
              try
              {
                 tm = TransactionManagerLocator.locateTransactionManager();
              }
              catch (Exception e)
              {
                 throw new Exception("TransactionManager unavailable", e);
              }
           }
        }
        return tm;
    }

    public void setTransactionManager(TransactionManager transactionManager)
    {
        this.tm = transactionManager;
    }

    public DataSource getDataSource() throws Exception
    {
        if(dataSource == null)
        {
           ControllerContext controllerContext = kernelControllerContext.getController().getInstalledContext("jbm:DataSource");
           if(controllerContext != null)
           {
              dataSource =  (DataSource) controllerContext.getTarget();
           }
           else
           {
              InitialContext ic = new InitialContext();
              //try in the initial context, if its not there use the one that has been injected
              try
              {
                 dataSource = (DataSource) ic.lookup("java:/DefaultDS");
              }
              catch (Exception e)
              {
                 throw new Exception("DataSource unavailable", e);
              }
           }

        }
        return dataSource;
    }

    public void setDataSource(DataSource datasource)
    {
        this.dataSource = datasource;
    }


   public TransactionManager getTm()
   {
      return tm;
   }

   public void setTm(TransactionManager tm)
   {
      this.tm = tm;
   }

   public AuthenticationManager getAuthenticationManager() throws Exception
   {
      if(authenticationManager == null)
        {
           ControllerContext controllerContext = kernelControllerContext.getController().getInstalledContext("jbm:AuthenticationManager");
           if(controllerContext != null)
           {
              authenticationManager = (AuthenticationManager) controllerContext.getTarget();
           }
           else
           {
              try
              {
                 InitialContext ic = new InitialContext();
                 authenticationManager = (AuthenticationManager)ic.lookup("java:/jaas/messaging");
              }
              catch (NamingException e)
              {
                 throw new Exception("AuthenticationManager unavailable", e);
              }
           }
        }
      return authenticationManager;
   }

   public void setAuthenticationManager(AuthenticationManager authenticationManager)
   {
      this.authenticationManager = authenticationManager;
   }

   public ObjectName getMultiplexer()
    {
        return multiplexer;
    }

    public void setMultiplexer(ObjectName multiplexer)
    {
        this.multiplexer = multiplexer;
    }
}
