package org.tutorial.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributeClient {

	private String connectString = "node1:2181,node2:2181,node3:2181";
	private int sessionTimeout = 2000;
	private ZooKeeper zk;

	public static void main(String[] args) throws Exception {
		DistributeClient client = new DistributeClient();
		// 1.取得zk連接
		client.getConnect();
		// 2.監聽 /servers/ 下面子節點的增加和刪除 ==> 監聽伺服器的上線下線情況
		client.getServerList();
		// 3.啟動業務邏輯(睡覺)
		client.business();
	}

	private void business() throws InterruptedException {
		Thread.sleep(Long.MAX_VALUE);
	}

	private void getServerList() throws KeeperException, InterruptedException {
		// 進行註冊監聽器..
		List<String> children = zk.getChildren("/servers", true);
		// 存放 /servers下的節點
		List<String> servers = new ArrayList<>();
		for (String child : children) {
			// 取得該節點上的內容不適用監聽器
			byte[] data = zk.getData("/servers/" + child, false, null);
			servers.add(new String(data));
		}
		System.out.println(servers);
	}

	private void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				try {
					getServerList();
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
}
