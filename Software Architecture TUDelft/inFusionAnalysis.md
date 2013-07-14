##Analysis with inFusion
To find possible design flaws in HornetQ, inFusion has been used.
inFusion is a tool which facilitates the analysis the architecture and it's design quality. The tool allows anyone to quickly get intuitive insights into possible inadiquate design decisions. In addition to finding these flaws, inFusion also provides matching refactoring recommendations.


###Approach
Using inFusion consists of three major steps. Analyzing the project, spotting code elements with design flaws, and last but not least, understandng the causes and impact of these earlier found flaws.
The analysis of the project is fairly straight forward. inFusion allows the analysis of entire projects. As it is not useful to analyze the design flaws in either test-cases or tests, these can be excluded easily. After the analysis is run, the results show.
These results can then be reviewed. inFusion returns a simple view with Quality Attributes and a "package distribution" view. These will be reviewed seperately.
The last step is understanding the causes and impacts of found problems. The purpose of this is to try to pinpoint the reason of the problem, if it has to be solved and possibly how.


###Spotting Design Flaws
There are two ways to spot design flaws with inFusion. The first is following quality attributes, the second is looking at the package distribution figure. 

#### Following quality attributes
First we will discuss the quality attributes as returned by inFusion, it is to be found in the "Quality Deficit Index" figure below.
The Quality Deficit Index of HornetQ is a 10,6. The larger this number, the more significant design problems have been detected. This also counts for the numbers below the five design aspects such as Cohesion.
To find flaws, it is recommended to pick the highest number (which, in our case, is 19) and proceeding to analyze that category. Explanation of this analysis continues in the next section.  

![quality deficit index](https://f.cloud.github.com/assets/3627314/673923/a083b034-d8c6-11e2-8411-ed15b7c11624.png)   
  
##### Quality Categories and analysis
The quality categories given in the quality deficit index figure are given in the table below. With these categories, the found design flaws and their frequency is given. Thense five categories have a large overlap in corresponding design flaws. Therefore these can occur in more than one category.
Because Coupling and Size and Complexity have a higher score, which indicates more significant design problems, it is interesting to investigate these. The most important design flaws in these categories are Duplication, God class, Cyclic dependencies and intensive Coupling. In the "Causes and Impact" section a God class and a class with intensive Coupling will be analyzed.

Category|Flaws and Frequency|Explanation
---|---|---
Size and Complexity | ![size_and_complexity_design_flaws](https://f.cloud.github.com/assets/3627314/673917/8a68f322-d8c6-11e2-89d9-7138f4230af5.png) | Duplication and God class design flaws are most important in this category. In total there are 338 design flaws in this category.
Coupling | ![coupling_design_flaws](https://f.cloud.github.com/assets/3627314/673924/a4b811ae-d8c6-11e2-8754-6f1d36cef052.png) | Cyclic dependencies, intensive Coupling and God class design flaws are most important in this category. In total there are 222 design flaws in this category.
Encapsulation | ![encapsulation_design_flaws](https://f.cloud.github.com/assets/3627314/673925/a4bd8242-d8c6-11e2-96b7-794c03c043b3.png) | Data Clumps design flaws are most important in this category. In total there are 223 design flaws in this category.
Hierarchies | ![hierarchies_design_flaws](https://f.cloud.github.com/assets/3627314/673926/a4bf0bda-d8c6-11e2-9960-9ebfb8d7051b.png) | Sibling duplication design flaws are most important in this category. In total there are 56 design flaws in this category.
Cohesion | ![cohesion_design_flaws](https://f.cloud.github.com/assets/3627314/673929/a81f81a6-d8c6-11e2-8c9c-24d9992e897f.png) | God class, Feature envy and Schizophrenic Class design flaws are most important in this category. In total there are 62 design flaws in this category.

#### Package distribution figure
The second strategy to find design flaws is looking into the Package Distribution Figure (below). This figure shows all packages with their classes. The with of a class block (the blocks in the lowest level) shows how much attributes it has. The height of these blocks show the number of methods they contain. The color of the block shows the criticality of design flaws, more red is worst.
To get to the most important design flaws, one should first look into the most red blocks. These are annotated in the figure. It is of no immediate importance how large the block is, when it it red it should be noted. Larger blocks contain more attributes and or methods, so they might influence more of the system and therefore can be more important.
Judging from this figure, it would be logical to start analyzing with "ClientSessionImpl", "ClusterManager", "HornetQServerImpl" or one of the other annotated classes. In the next session we'll go into the ClusterManager.  

![package map_annotated](https://f.cloud.github.com/assets/3627314/673922/a07f57fa-d8c6-11e2-8b3a-bc5f5cb3b12e.png) 

###Causes and Solutions
In this section we are going to take a closer look at a method and a class. The setParams method and the ClusterManager class. setParams has problems with the "Intensive Coupling" design flaw. ClusterManager is categorized as a God Class.
setParams and ClusterManager have been chosen because they have the highest severity rate in their categories. As to be seen below, these classes are not the only one. There are at least four other severe God Classes and at least one other  method with severe Intensive Coupling.  
  
__Top 5 Intensive Coupling__  
![top_5_intensive_coupling_coupling](https://f.cloud.github.com/assets/3627314/673919/8a82bf1e-d8c6-11e2-962a-2217243ef9c6.png)  
  
__Top 5 God Classes__  
![top_5_god_class_coupling](https://f.cloud.github.com/assets/3627314/673918/8a7efaaa-d8c6-11e2-9386-96f316fb5f3c.png) 

####setParams
As described in the image below, setParams suffers from Intensive Coupling because it calls a large number of methods from one or more external classes. This problem can be solved by adding higher level services which replace methods in classes from which setParams calls these methods.  
In case of setParams it might not be necessary to solve this design issue. This because setParams is meant to be highly coupled to other methods with or for which it sets the parameters. Higher level services might still be a good addition to solve this issue.  
  
![setparams_intensive_coupling](https://f.cloud.github.com/assets/3627314/673916/8a52efbe-d8c6-11e2-97e6-17f38972b47d.png)  

####ClusterManager
The ClusterManager Class suffers from the God Class design flaw. It uses many attributes from external classes. To solve this problem, inFusion provides a few steps that can be taken.  
One of these steps is to "Explore the Encapsulation View to spot the methods and attributes of the class that are the cause of low encapsulation.". The Encapsulation View and it's analysis is included at the bottom of this section.  
The class is also extensively large and complex, this could be solved by reducing cyclomic complexity and reducing the nesting within the class.
At last, the class is non-cohesive. Methonds within this class do not use many private attributes. This is possibly not an issue because it could be an aspect of the ClusterManager.
  
![clustermanager_god_class](https://f.cloud.github.com/assets/3627314/673930/a81eb7d0-d8c6-11e2-828d-f008e9496f48.png)  
  
#####Encapsulation View
The Encapsulation View (below) shows blocks which represent either methods or attributes. For methods, the height of a block represents the lines of code within that method. In case of an attribute, the height represents the number of accesses. 
The top left cluster of blocks represent the publicly accessible methods. As you can see there are more of these than there are private methods. To reduce the God Class issue, these methods or functionality thereof should be moved, if possible, to be more private.
The lower levels within the Encapsulation View show all collaborator classes. The sheer number of these classes show that the ClusterManager is a God Class.
Because ClusterManager is restricted by functionality to have many collaborator classes, the God Class design flaw might not be easily solved for the ClusterManager.
  
![clustermanager_god_class_encapsulation_view](https://f.cloud.github.com/assets/3627314/673931/a8207098-d8c6-11e2-9c86-78540484a517.png)  










