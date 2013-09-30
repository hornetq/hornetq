clear all
close all
clc
files = {'2-0-0-beta5','2-0-0-CR1','2-0-0-CR2','2-1-0-CR1','2-1-0-Final','2-1-2-Final','2-2-2-Final','2-2-5-Final','2-2-14-Final'};

for i = 1 : length(files)
    file = char(files(i));
    fid = fopen([file '.txt']);    
    tline = fgetl(fid);
    func = [];
    while ischar(tline)
        [id,comp1] = strtok(tline,'   ');
        [~,comp2]=strtok(comp1,'   ');
        [comp,~] = strtok(comp2,'   ');
        func(str2num(id))=str2num(comp);
        tline = fgetl(fid);
    end
    fclose(fid);

    sim = length(find(func<=10))/length(func);
    moder = length(find((func>10)&(func<=20)))/length(func);
    highr = length(find((func>20)&(func<=50)))/length(func);
    vhighr = length(find(func>50))/length(func);
    figure,pie([sim,moder,highr,vhighr],[0,0,0,1]),legend(strcat('Simple-',num2str(sim*100),'%'),strcat('Moderate-',num2str(moder*100),'%'),...
        strcat('High Risk-',num2str(highr*100),'%'),strcat('Very High Risk-',num2str(vhighr*100),'%'),'Location','SouthEastOutside')
    print('-dpng',[file '.png']);
    figure,hist(func,100)
    print('-dpng',[file 'histogram.png'])
end