package cn.zhuzi.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author 作者 grq
 * @version 创建时间：2018年7月9日 下午7:26:33
 *
 */
public class ZKServer {

	private String connString = "nan:2181";
	ZooKeeper zooKeeper;
	private String parentNode = "/servers";

	public void getConnection() throws IOException {
		zooKeeper = new ZooKeeper(connString, 2000, new Watcher() {

			@Override
			public void process(WatchedEvent event) {

			}
		});
	}

	/**
	 * 注册
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void regist(String hostName) throws KeeperException, InterruptedException {
		Stat exists = zooKeeper.exists(parentNode, false);
		if (exists == null) {
			zooKeeper.create(parentNode, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		zooKeeper.create(parentNode + "/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName + " is online@!");
	}

	public void business() throws InterruptedException {
		System.out.println("ZKServer.business()");
		Thread.sleep(Long.MAX_VALUE);

	}

	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		ZKServer zkServer = new ZKServer();
		zkServer.getConnection();
		zkServer.regist(args[0]);
		zkServer.business();
	}
}
