import numpy as np
import multiprocessing
import Queue
import matplotlib as mpl
mpl.use('gtkagg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.colors as colors


class HandoffHeatmap(multiprocessing.Process):
	def __init__(self, base_mem_size, base_disk_size, dirty_mem_queue, dirty_disk_queue):
		super(HandoffHeatmap, self).__init__()
		self.dirty_mem_queue = dirty_mem_queue
		self.dirty_disk_queue = dirty_disk_queue
		self.mem_size = base_mem_size
		self.disk_size = base_disk_size
		self.mem_chunks = self.mem_size / 4096
		self.disk_chunks = self.disk_size / 4096
		self.scaling = 1
		self.mem_ydim = np.ceil(np.sqrt(self.mem_chunks) / self.scaling)
		self.mem_canvas = [np.sqrt(self.mem_chunks) / self.scaling, (np.sqrt(self.mem_chunks) * self.scaling)]
		self.disk_ydim = np.ceil(np.sqrt(self.disk_chunks) / self.scaling)
		self.disk_canvas = [np.sqrt(self.disk_chunks) / self.scaling, (np.sqrt(self.disk_chunks) * self.scaling)]

		self.cmap = colors.ListedColormap(['white', 'black', 'grey', 'firebrick'])
		self.bounds=[1, 2, 3, 4, 5]
		self.norm = colors.BoundaryNorm(self.bounds, self.cmap.N)

		self.a = np.random.random_integers(1, size=self.mem_canvas)
		self.b = np.random.random_integers(1, size=self.disk_canvas)

	def updatemem(self, *args):
			#x = np.random.random_integers(np.sqrt(self.mem_chunks) / self.scaling)
			#y = np.random.random_integers(self.scaling * np.sqrt(self.mem_chunks))
			try:
				while self.dirty_mem_queue.empty() == False:
					pages = self.dirty_mem_queue.get(False)
					for address in pages:
						x = np.floor(address / self.mem_ydim)
						y =  address - (x * self.mem_ydim)
						self.a[x, y-1] = 2
				self.mem_im.set_array(self.a)
			except Queue.Empty:
				pass
			return self.mem_im,

	def updatedisk(self, *args):
			#x = np.random.random_integers(np.sqrt(self.disk_chunks) / self.scaling)
			#y = np.random.random_integers(self.scaling * np.sqrt(self.disk_chunks))
			try:
				while self.dirty_disk_queue.empty() == False:
					pages = self.dirty_disk_queue.get(False)
					for address in pages:
						x = np.floor(address / self.disk_ydim)
						y =  address - (x * self.disk_ydim)
						self.b[x, y-1] = 2
				self.disk_im.set_array(self.b)
			except Queue.Empty:
				pass
			return self.disk_im,

	def run(self):
		fig = plt.figure()
		fig.suptitle("Memory Snapshot\n%d MB\n%d 4KB chunks" % (self.mem_size / (1024*1024), self.mem_chunks), fontsize=10, fontweight='bold')
		self.mem_im = plt.imshow(self.a, cmap=self.cmap, interpolation='none', animated=True, norm=self.norm)
		ani = animation.FuncAnimation(fig, self.updatemem, interval=100, blit=True)
		# legend
		mem_legend = plt.colorbar(self.mem_im, cmap=self.cmap, norm=self.norm,  ticks=[1.5,2.5,3.5,4.5], orientation='horizontal')
		mem_legend.ax.set_xticklabels(['Unmodified', 'Modified', 'Decompressed', 'Delta Applied'])  

		fig2 = plt.figure()
		fig2.suptitle("Disk Snapshot\n%d MB\n%d 4KB chunks" % (self.disk_size / (1024 * 1024), self.disk_chunks), fontsize=10, fontweight='bold')
		self.disk_im = plt.imshow(self.b, cmap=self.cmap, interpolation='none', animated=True, norm=self.norm)
		ani2 = animation.FuncAnimation(fig2, self.updatedisk, interval=100, blit=True)
		# legend
		disk_legend = plt.colorbar(self.disk_im, cmap=self.cmap, norm=self.norm,  ticks=[1.5,2.5,3.5,4.5], orientation='horizontal')
		disk_legend.ax.set_xticklabels(['Unmodified', 'Modified', 'Decompressed', 'Delta Applied'])  
		plt.show()


# mqueue = multiprocessing.Queue()
# dqueue = multiprocessing.Queue()
# sample = HandoffHeatmap(1024*1024, 1024*1024*8, mqueue, dqueue)
# sample.daemon = True
# sample.start()
# i = 1
# while i < 2048:
# 	dqueue.put([i])
# 	i += 1

# while 1:
# 	pass
# sample.terminate()
