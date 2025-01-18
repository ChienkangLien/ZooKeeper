package org.tutorial.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorLock {

	public static void main(String[] args) {
		InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");
		InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lock1.acquire();
					System.out.println("線程 1 獲取鎖");
					lock1.acquire();
					System.out.println("線程 1 再次獲取鎖");
					Thread.sleep(5 * 1000);
					lock1.release();
					System.out.println("線程 1 釋放鎖");
					lock1.release();
					System.out.println("線程 1 再次釋放鎖");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lock2.acquire();
					System.out.println("線程 2 獲取鎖");
					lock2.acquire();
					System.out.println("線程 2 再次獲取鎖");
					Thread.sleep(5 * 1000);
					lock2.release();
					System.out.println("線程 2 釋放鎖");
					lock2.release();
					System.out.println("線程 2 再次釋放鎖");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	private static CuratorFramework getCuratorFramework() {
		ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString("node1:2181,node2:2181,node3:2181")
				.connectionTimeoutMs(2000) // 設定連線逾時時間
				.sessionTimeoutMs(2000).retryPolicy(policy).build(); // 嘗試策略
		
		// 啟動客戶端
		client.start();
		System.out.println("Zookeeper啟動成功..");
		
		return client;
	}
}
