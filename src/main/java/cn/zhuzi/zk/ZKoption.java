package cn.zhuzi.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 作者 grq
 * @version 创建时间：2018年7月9日 下午4:30:30
 * @see zookeeper 练习
 */
public class ZKoption {
	private String conn = "xt216:2181,xt216:2182,xt216:2183";
	int sessionTimeout = 2000;
	ZooKeeper keeper;

	@Before
	public void init() throws IOException {

		keeper = new ZooKeeper(conn, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.toString());
			}
		});
	}

	/**
	 * 无法递归创建
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void createNode() throws KeeperException, InterruptedException {
		/**
		 * 参数1 路劲 参数2 数据 参数3 ACL 参数3 节点类型
		 */
		String create = keeper.create("/data/grq", "data file".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(create);
	}
	
	@Test
	public void getNode() throws KeeperException, InterruptedException{
		/**
		 * 参数1 节点
		 * 参数2 是否启动监听
		 * 参数3 状态
		 */
		byte[] data = keeper.getData("/data", false, null);
		System.out.println(new String(data));
		List<String> children = keeper.getChildren("/", false);
		for (String string : children) {
			System.out.println(string);
		}
	}
	/**
	 * 判断节点是否存在
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void isexist() throws KeeperException, InterruptedException{
		Stat exists = keeper.exists("/data", false);
		System.out.println(exists);
	}
}
