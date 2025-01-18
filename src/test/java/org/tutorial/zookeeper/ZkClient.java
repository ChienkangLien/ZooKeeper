package org.tutorial.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZkClient {

	// 注意：逗號前後不能有空格
	private static String connectString = "node1:2181,node2:2181,node3:2181";
	private static int sessionTimeout = 2000;
	private ZooKeeper zkClient = null;

	// 連接ZooKeeper
	@Before
	public void init() throws Exception {
		/**
		 * connectString：要連接的Zookeeper客戶端
		 * sessionTimeout：超時時間
		 * Watcher：監聽器
		 */
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				System.out.println("--------------------------------");
				// 這裡意思就是你只要執行zk的api指令，就會走監聽器重寫的方法，最後加一個延遲，主執行緒睡眠但是監聽器還在
				// 本質：是讓監聽器執行緒一直存在著...
				// 再次啟動監聽...
				List<String> children = null;
				try {
					children = zkClient.getChildren("/", true);
				} catch (Exception e) {
					e.printStackTrace();
				}
				for (String child : children) {
					System.out.println(child);
				}
				System.out.println("--------------------------------");
			}
		});
	}

	// 建立節點
	@Test
	public void create() throws Exception {
		/**
		 * 參數 1：要建立的節點的路徑
		 * 參數 2：節點資料
		 * 參數 3：節點權限
		 * 參數 4：節點的類型
		 */
		zkClient.create("/tutorial", "taipei".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	// 監聽節點變化
	// 執行程式後、到shell命令客戶端操作節點的建立和刪除並觀察控制台
	// 例如create /a1 "a1"以及delete a1
	@Test
	public void getChildren() throws Exception {
		// true：使用init中建立的監聽器。每次出現變化，將重新呼叫監聽器中的方法。也可以自定一個監聽器。
		// 監聽某個路徑的節點變化狀況
		List<String> children = zkClient.getChildren("/", true);
		for (String child : children) {
			System.out.println(child);
		}
		// 延時、讓其一直監聽
		Thread.sleep(Long.MAX_VALUE);
	}

	@Test
	public void exist() throws Exception {
		Stat stat = zkClient.exists("/a1", false); // 執行時可將process中程式注解掉、才不會真正執行監聽器
		System.out.println(stat == null ? "not exist" : "exist");
	}
}
