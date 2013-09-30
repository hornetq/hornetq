%% Other metrics processing
clear all
close all
clc
load metrics

first_order_density_mean = mean(metrics(1,:))
first_order_density_median = median(metrics(1,:))
first_order_density_stdev = std(metrics(1,:))
first_order_density_min = min(metrics(1,:))
first_order_density_max = max(metrics(1,:))

propagation_cost_mean = mean(metrics(2,:))
propagation_cost_median = median(metrics(2,:))
propagation_cost_stdev = std(metrics(2,:))
propagation_cost_min = min(metrics(2,:))
propagation_cost_max = max(metrics(2,:))

core_size_mean = mean(metrics(3,:))
core_size_median = median(metrics(3,:))
core_size_stdev = std(metrics(3,:))
core_size_min = min(metrics(3,:))
core_size_max = max(metrics(3,:))

LOC_mean = mean(metrics(4,:))
LOC_median = median(metrics(4,:))
LOC_stdev = std(metrics(4,:))
LOC_min = min(metrics(4,:))
LOC_max = max(metrics(4,:))

Cyclo_mean = mean(metrics(5,:))
Cyclo_median = median(metrics(5,:))
Cyclo_stdev = std(metrics(5,:))
Cyclo_min = min(metrics(5,:))
Cyclo_max = max(metrics(5,:))     

figure,hold on,plot(metrics(1,:),'-'),plot(metrics(1,:),'r*'),hold off,xlabel('HornetQ release'),ylabel('First-order density')
print('-dpng','First-order-density.png');
figure,hold on,plot(metrics(2,:),'-'),plot(metrics(2,:),'r*'),hold off,xlabel('HornetQ release'),ylabel('Propagation cost')
print('-dpng','Propagation-cost.png');
figure,hold on,plot(metrics(3,:),'-'),plot(metrics(3,:),'r*'),hold off,xlabel('HornetQ release'),ylabel('Core-size')
print('-dpng','Core-size.png');
figure,hold on,plot(metrics(4,:),'-'),plot(metrics(4,:),'r*'),hold off,xlabel('HornetQ release'),ylabel('LOC')
print('-dpng','LOC.png');
figure,hold on,plot(metrics(5,:),'-'),plot(metrics(5,:),'r*'),hold off,xlabel('HornetQ release'),ylabel('Cyclomatic Complexity')
print('-dpng','Cyclomatic-complexity.png');