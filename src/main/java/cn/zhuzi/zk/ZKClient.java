package cn.zhuzi.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author 作者 grq
 * @version 创建时间：2018年7月9日 下午7:36:53
 *
 */
public class ZKClient {
	private String connString = "nan:2181";
	ZooKeeper zkCli;
	private String parentNode = "/servers";

	public void getConnection() throws IOException {
		zkCli = new ZooKeeper(connString, 2000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				try {
					getServerList();
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	public void getServerList() throws KeeperException, InterruptedException {
		ArrayList<String> server = new ArrayList<String>(16);

		List<String> children = zkCli.getChildren(parentNode, true);
		for (String string : children) {
			byte[] data = zkCli.getData(parentNode + "/" + string, false, null);
			server.add(new String(data));
		}
		System.out.println(server);
	}

	public void business() throws InterruptedException {
		System.out.println(" clinet---------------");
		Thread.sleep(Long.MAX_VALUE);

	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		// 获取连接
		ZKClient client = new ZKClient();
		// 监听节点变化
		client.getConnection();
		// 业务逻辑处理
		client.getServerList();
		client.business();
	}
}
