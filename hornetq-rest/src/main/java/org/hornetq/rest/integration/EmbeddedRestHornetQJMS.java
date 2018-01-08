package org.hornetq.rest.integration;

import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.jms.server.embedded.EmbeddedJMS;
import org.hornetq.spi.core.naming.BindingRegistry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedRestHornetQJMS extends EmbeddedRestHornetQ
{

   public EmbeddedRestHornetQJMS(ConnectionFactoryOptions jmsOptions)
   {
      super(jmsOptions);
   }


   @Override
   protected void initEmbeddedHornetQ()
   {
      embeddedHornetQ = new EmbeddedJMS();
   }

   public BindingRegistry getRegistry()
   {
      if (embeddedHornetQ == null) return null;
      return ((EmbeddedJMS)embeddedHornetQ).getRegistry();
   }

}
