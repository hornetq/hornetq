/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common;

import javax.naming.InitialContext;
import javax.naming.Context;
import java.util.Properties;

/**
 * @author Clebert Suconic
 */
public abstract class ExampleSupportCluster extends ExampleSupport {

    /** This will get the Context specific to a cluster */
    public InitialContext getServerContext(int serverNumber) throws Exception
    {
        String serverIP = System.getProperty("CLUSTER_IP_" + serverNumber);
        if (serverIP==null)
        {
            String errorMessage = "You should define CLUSTER_IP_" + serverNumber + " in your java properties or in your server.properites at your example's directory";
            System.out.println(errorMessage);
            throw new Exception(errorMessage);
        }
        return getServerContext(serverIP);
    }

    public InitialContext getServerContext(String serverIP) throws Exception
    {
        String jndiProviderClass = "org.jnp.interfaces.NamingContextFactory";
        Properties contextProperties = new Properties();
        contextProperties.put(Context.INITIAL_CONTEXT_FACTORY,
            jndiProviderClass);
        contextProperties.put(Context.PROVIDER_URL,
            "jnp://" + serverIP+ ":1099");
        return new InitialContext(contextProperties);
    }



    protected void run()
    {
       try
       {
          InitialContext ct1 = getServerContext(1);
          InitialContext ct2 = getServerContext(2);
           setup(ct1);
           setup(ct2);
          example();
           tearDown(ct1);
           tearDown(ct2);
       }
       catch(Throwable t)
       {
          t.printStackTrace();
          setFailure(true);
       }

       reportResultAndExit();
    }

}
