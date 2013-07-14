
files = {'HornetQ-2.0.0.BETA5-src','hornetq-2.0.0.CR1-src','hornetq-2.0.0.CR2-src',...
    'hornetq-2.0.0.GA-src','hornetq-2.1.0.CR1-src','hornetq-2.1.2.Final-src',...
    'hornetq-2.2.2.Final-src','hornetq-2.2.5.Final-src','hornetq-2.2.14.Final-src'};
for jj = 1 : length(files)
file = char(files(jj));
mkdir(file);

fid = fopen(['C:\Users\Madalin\Desktop\Mircea\Edges\' file '.txt']);

tline = fgetl(fid);
i = 0;
A = [];

while ischar(tline)
    [e1,e2] = strtok(tline,'->');
    e2 = e2(3:end);
    e1 = str2num(e1)+1;
    e2 = str2num(e2)+1;
    A(e1,e2) = 1;
    A(e2,e1) = 0;
    tline = fgetl(fid);
end
fclose(fid);
% compute density of the matrix
first_order_density = size(find(A~=0),1) / (size(A,1) * size(A,2));
disp([file, ' first_order_density'])
disp(first_order_density)

figure,imshow(A),title(['First-order dependency matrix ' file]);
print('-dpng',[file '\First-order dependency matrix ' file '.png']);
D = all_shortest_paths(sparse(A));
p = max(D(D~=inf));

B = zeros(size(A));
for i = 1 : p
    B = B + A^i;
end

C = B;
C(find(B~=0))=1;
figure,imshow(C),title(['Visibility matrix ' file]);
print('-dpng',[file '\Visibility matrix ' file, '.png']);

propagation_cost = size(find(B~=0),1) / (size(B,1) * size(B,2));
disp([file, ' propagation_cost'])
disp(propagation_cost)

for i = 1 : size(B)
    fan_in(i) = numel(find(B(:,i)~=0));
    fan_out(i) = numel(find(B(i,:)~=0));
end

figure,plot(1:length(fan_in),sort(fan_in),'-*'),title(['Fan-in ' file]);
print('-dpng',[file '\Fan-in ' file, '.png']);
figure,plot(1:length(fan_out),sort(fan_out),'-*'),title(['Fan-out ' file]);
print('-dpng',[file '\Fan-out ' file, '.png']);

filesMat = zeros(1+max(fan_out),1+max(fan_in));
for i = 1 : numel(fan_out)
    filesMat(1+fan_out(i),1+fan_in(i)) = filesMat(1+fan_out(i),1+fan_in(i)) + 1;
end

figure, imshow(filesMat),axis on,title(['File types ' file]);
print('-dpng',[file '\File types ', file, '.png']);
sfanout = sort(fan_out);
sfanin = sort(fan_in);
for i = 2 : numel(fan_out)
    delta(i)=sfanout(i)-sfanout(i-1);
end
[h1,c1] = hist(delta,5);
[mh1,j] = max(h1);
i1 = -max(delta)/5 /2 + c1(j);
i2 = max(delta)/5 /2+ c1(j);
for k1 = 1 : numel(delta)
    if (delta(k1) < i1) || (delta(k1)>i2)
        break
    end
end

for i = 2 : numel(fan_in)
    delta(i)=sfanin(i)-sfanin(i-1);
end
[h1,c1] = hist(delta,5);
[mh1,j] = max(h1);
i1 = -max(delta)/5 /2 + c1(j);
i2 = max(delta)/5 /2+ c1(j);
for k2 = 1 : numel(delta)
    if (delta(k2) < i1) || (delta(k2)>i2)
        break
    end
end

out1 = sfanout(k1-1);
in1 = sfanin(k2-1);

core_size = 0;
for i = 1 : numel(fan_out)
    if fan_out(i)>out1 && fan_in(i)>in1
        core_size = core_size + 1;
    end
end
disp([file, ' core_size'])
disp(core_size)
end