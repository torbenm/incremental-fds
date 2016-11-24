package org.mp.naumann.algorithms.fd.utils;

import java.lang.management.ManagementFactory;
import java.util.logging.Logger;

import org.mp.naumann.algorithms.fd.structures.FDList;
import org.mp.naumann.algorithms.fd.structures.FDSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;

public class MemoryGuardian {

	private final static Logger LOG = Logger.getLogger(MemoryGuardian.class.getName());

    private final long AVAILABLE_MEMORY = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    private final float MAX_MEMORY_USAGE_PERCENTAGE = 0.8f;
    private final float TRIM_MEMORY_USAGE_PERCENTAGE = 0.7f;

    private final long MAX_MEMORY_USAGE = (long)(AVAILABLE_MEMORY * MAX_MEMORY_USAGE_PERCENTAGE);
    private final long TRIM_MEMORY_USAGE = (long)(AVAILABLE_MEMORY * TRIM_MEMORY_USAGE_PERCENTAGE);
    private final long MEMORY_CHECK_FREQUENCY = (long)Math.max(Math.ceil((float) AVAILABLE_MEMORY / 10000000), 10);

	private boolean active;
	private int allocationEventsSinceLastCheck = 0;

	public MemoryGuardian(boolean active) {
		this.active = active;
	}
	
	public void memoryChanged(int allocationEvents) {
		this.allocationEventsSinceLastCheck += allocationEvents;
	}

	private boolean memoryExhausted(long memory) {
		long memoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
		return memoryUsage > memory;
	}
	
	public void match(FDSet negCover, FDTree posCover, FDList newNonFDs) {
		if ((!this.active) || (this.allocationEventsSinceLastCheck < this.MEMORY_CHECK_FREQUENCY))
			return;
		
		if (this.memoryExhausted(this.MAX_MEMORY_USAGE)) {
			Runtime.getRuntime().gc();

			while (this.memoryExhausted(this.TRIM_MEMORY_USAGE)) {
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
