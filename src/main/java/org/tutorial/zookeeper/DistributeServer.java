package org.tutorial.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class DistributeServer {

	private String connectString = "node1:2181,node2:2181,node3:2181";
	private int sessionTimeout = 2000;
	private ZooKeeper zk;

	public static void main(String[] args) throws Exception {
		DistributeServer server = new DistributeServer();
		// 1.取得zk連線 ==> 連線zk客戶端
		server.getConnect();
		// 2.註冊伺服器到zk叢集，在/servers下建立節點
		server.regist(args[0]);
		// 3.啟動業務邏輯(睡覺)
		server.business();
	}

	private void business() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}

	private void regist(String hostname) throws KeeperException, InterruptedException {
		// 節點類型應該是暫時的(上線建立節點,下線節點消失), 小區的(可以知道伺服器上線的順序)
		zk.create("/servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + " is online...");
	}

	private void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
			}
		});
	}
}
