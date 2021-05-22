from os import rename
import matplotlib.pyplot as plt
import seaborn as sns

#results stored in a list of lists
#one list stores for each thread, it stores avg of the endtime-startime for all workers 1-5 

res = []

# for i in range(1,5):
#     temp = []
#     for j in range(1,6):
#         with open("results_dir/results_" + str(j) + "_" + str(i)) as fp:
#             f = fp.readlines()
#             diff_list = []
#             for string in f:
#                 x = string.split(":")[0]
#                 starttime = float(x.split(",")[0])
#                 endtime = float(x.split(",")[1])
#                 diff = endtime - starttime
#                 diff_list.append(diff)

#             avg_val = sum(diff_list)/len(diff_list)
#             temp.append(avg_val)

#     res.append(temp)



# print(res)
    
for i in range(1,5):

    with open("results_dir/results_" +  str(i*4)) as fp:
        f = fp.readlines()
        diff_list = []
        for string in f:
            x = string.split(":")[0]
            starttime = float(x.split(",")[0])
            endtime = float(x.split(",")[1])
            diff = endtime - starttime
            diff_list.append(diff)

        avg_val = sum(diff_list)/len(diff_list)
    res.append(avg_val)



print(res)

# for i in range(4):
x = [x*4 for x in range(1,5)]
y = [x*1000 for x in res]
plt.title("Average GET request response time(ms) vs num_threads")
plt.xlabel("Number of threads")
plt.ylabel("Time taken")
plt.yticks(y)
plt.plot(x,y,marker = 'o',c = 'g')
plt.show()



