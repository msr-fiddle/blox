import matplotlib.pyplot as plt

# {'med_jobs_new_flow': defaultdict(<class 'dict'>, {'Las': {20.0: 20084.94205794206, 21.0: 20119.82917082917, 22.0: 20973.226773226772}, 'Fifo': {20.0: 20084.64235764236, 21.0: 20090.158841158842, 22.0: 20126.573426573428, 23.0: 21105.965034965036, 24.0: 27817.017982017984}})})


fifo_list = []
fifo_list.extend([20084.6] * 20)
fifo_list.extend([20090, 20126, 21105, 27817])
las_list = []
las_list.extend([20084.6] * 20)
las_list.extend([20084, 20119, 20973.226773226772, 152000])
srtf_list = []
srtf_list.extend([20084.6] * 20)
srtf_list.extend([20084, 20084, 21005, 26817])
# plt.plot([1, 2, 3, 4], "o-r")
plt.plot(range(len(fifo_list)), fifo_list, label="Fifo")
plt.plot(range(len(srtf_list)), srtf_list, label="Srtf")
plt.plot(range(len(las_list)), las_list, label="Srtf")
plt.xlabel("Load jobs/hr")
plt.ylabel("Avg JCT (s)")
plt.legend()
plt.show()
