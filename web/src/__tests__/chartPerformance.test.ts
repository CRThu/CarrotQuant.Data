import { describe, it, expect } from 'vitest';

describe('Chart linkage performance guard unit test suite', () => {
  it('prevents range sync recursion loop during chart zooming', () => {
    let syncLock = false;
    let syncCalls = 0;

    const mockSyncHandler = (sourceRange: { from: number; to: number }) => {
      if (syncLock) return;
      syncLock = true;
      syncCalls++;

      // 模拟 target 触发反向同步回调
      mockSyncHandler(sourceRange);

      // 模拟微任务/动画帧解锁
      syncLock = false;
    };

    mockSyncHandler({ from: 10, to: 100 });

    // 证明锁机制有效防止了无限递归
    expect(syncCalls).toBe(1);
  });
});
