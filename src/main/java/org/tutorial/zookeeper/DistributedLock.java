package org.tutorial.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributedLock {

	private final String connectString = "node1:2181,node2:2181,node3:2181";
	private final int sessionTimeout = 2000;
	private final ZooKeeper zk;

	// CountDownLatch 用來等待連線的成功
	private CountDownLatch connectLatch = new CountDownLatch(1);
	private CountDownLatch waitLatch = new CountDownLatch(1);
	private String currentMode;
	private String waitPath;

	public DistributedLock() throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// connection如果連接上zk，可以釋放
				// 如果監聽的狀態是連線上的狀態，則釋放connectLatch，繼續往下執行
				if (event.getState() == Event.KeeperState.SyncConnected) {
					connectLatch.countDown();
				}

				// waitLatch需要釋放
				// 如果監聽了監聽路徑的節點刪除操作、且該操作的路徑是目前節點的上一個節點，則釋放waitLatch
				if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
					waitLatch.countDown();
				}
			}
		});

		// 等待zk正常連線後，往下走程序
		connectLatch.await();

		// 判斷根節點 /locks 是否存在
		Stat stat = zk.exists("/locks", false);

		// 如果根節點不存在，則創建根節點，根節點類型為永久節點
		if (stat == null) {
			zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	// 加鎖方法
	public void zkLock() throws KeeperException, InterruptedException {
		// 建立對應的臨時帶序號節點(目的是對資源進行操作.)
		currentMode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

		// 判斷所建立的節點是否為最小的序號節點，如果是取得到鎖；如果不是，監聽序號前一個節點
		List<String> children = zk.getChildren("/locks", false);

		// 如果children只有一個值那就直接取得鎖；如果有多個節點則需要判斷誰最小
		if (children.size() == 1) {
			return;
		} else {
			Collections.sort(children);
			// 取得節點名稱 seq-000000
			String thisNode = currentMode.substring("/locks/".length());
			// 透過seq-000000取得該節點在children集合的位置
			int index = children.indexOf(thisNode);

			// 判斷
			if (index == -1) {
				System.out.println("資料異常...");
			} else if (index == 0) {
				// 如果目前的是序號最小的節點,則直接取得鎖
				return;
			} else {
				// 當前的節點並不是序號最小的，則需要監聽前一個節點的變化
				waitPath = "/locks/" + children.get(index - 1);
				zk.getData(waitPath, true, null);

				// 等待前一個節點操作完成，監聽結束，本節點再取得鎖定
				waitLatch.await();

				return;
			}
		}
	}

	// 解鎖方法
	public void zkUnlock() throws InterruptedException, KeeperException {
		// 操作處理完畢要解鎖，刪除目前節點
		zk.delete(this.currentMode,-1);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		final DistributedLock lock1 = new DistributedLock();
		final DistributedLock lock2 = new DistributedLock();

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lock1.zkLock();
					System.out.println("線程 1 獲取鎖");
					Thread.sleep(5 * 1000);
					lock1.zkUnlock();
					System.out.println("線程 1 釋放鎖");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lock2.zkLock();
					System.out.println("線程 2 獲取鎖");
					Thread.sleep(5 * 1000);
					lock2.zkUnlock();
					System.out.println("線程 2 釋放鎖");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
}
