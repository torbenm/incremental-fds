package org.mp.naumann.algorithms.fd.hyfd;

import java.lang.management.ManagementFactory;
import java.util.logging.Logger;

import org.mp.naumann.algorithms.fd.structures.FDTree;

class MemoryGuardian {

	private final static Logger LOG = Logger.getLogger(MemoryGuardian.class.getName());

	private boolean active;
	private long memoryCheckFrequency;						// Number of allocation events that cause a memory check
	private long maxMemoryUsage;
	private long trimMemoryUsage;
	private int allocationEventsSinceLastCheck = 0;

	public MemoryGuardian(boolean active) {
		this.active = active;
		long availableMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
		float maxMemoryUsagePercentage = 0.8f;
		this.maxMemoryUsage = (long)(availableMemory * maxMemoryUsagePercentage);
		float trimMemoryUsagePercentage = 0.7f;
		this.trimMemoryUsage = (long)(availableMemory * trimMemoryUsagePercentage);
		this.memoryCheckFrequency = (long)Math.max(Math.ceil((float) availableMemory / 10000000), 10);
	}
	
	public void memoryChanged(int allocationEvents) {
		this.allocationEventsSinceLastCheck += allocationEvents;
	}

	private boolean memoryExhausted(long memory) {
		long memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
		return memoryUsage > memory;
	}
	
	public void match(FDSet negCover, FDTree posCover, FDList newNonFDs) {
		if ((!this.active) || (this.allocationEventsSinceLastCheck < this.memoryCheckFrequency))
			return;
		
		if (this.memoryExhausted(this.maxMemoryUsage)) {
//			LOG.info("Memory exhausted (" + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() + "/" + this.maxMemoryUsage + ") ");
			Runtime.getRuntime().gc();
//			LOG.info("GC reduced to " + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
			
			while (this.memoryExhausted(this.trimMemoryUsage)) {
				int depth = Math.max(posCover.getDepth(), negCover.getDepth()) - 1;
				if (depth < 1)
					throw new RuntimeException("Insufficient memory to calculate any result!");
				
				LOG.info(" (trim to " + depth + ")");
				posCover.trim(depth);
				negCover.trim(depth);
				if (newNonFDs != null)
					newNonFDs.trim(depth);
				Runtime.getRuntime().gc();
			}
		}
		
		this.allocationEventsSinceLastCheck = 0;
	}
}
