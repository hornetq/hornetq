package org.hornetq.integration.spring;

import org.hornetq.jms.server.embedded.EmbeddedJMS;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringJmsBootstrap extends EmbeddedJMS implements BeanFactoryAware
{
   public void setBeanFactory(BeanFactory beanFactory) throws BeansException
   {
      registry = new SpringBindingRegistry((ConfigurableBeanFactory) beanFactory);
   }
}
