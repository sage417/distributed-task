package moe.yamato.task.worker.master;

import moe.yamato.task.worker.common.watcher.ZkWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.UUID;

public class MasterWatcher extends ZkWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterWatcher.class);

    private static final String serverId = UUID.randomUUID().toString();

    private static volatile boolean isMaster;

    @Inject
    public MasterWatcher(final String hostPort) {
        super(hostPort);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, (rc, path, ctx, name) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    isMaster = true;
                    break;
                case NODEEXISTS:
                    break;
                case CONNECTIONLOSS:
                    LOGGER.warn("connection loss when create /master");
                    checkMaster();
                    break;
                default:
                    LOGGER.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }, null);
    }

    private void checkMaster() {
        zk.getData("/master", false, (rc, path, ctx, data, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    break;
                case NONODE:
                    runForMaster();
                    break;
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                default:
                    LOGGER.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }, null);
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (rc, p, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createParent(p, (byte[]) ctx);
                            break;
                        case OK:
                            break;
                        case NODEEXISTS:
                            break;
                        default:
                    }
                },
                data);
    }

    void listenWorkers() {
        zk.getChildren("/workers", event -> {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals(event.getPath());
                listenWorkers();
            }
        }, (rc, path, ctx, children) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //getWorkerList();
                    break;
                case OK:
                    LOGGER.info("Succesfully got a list of workers: "
                            + children.size()
                            + " workers");
                    //reassignAndSet(children);
                    break;
                default:
                    LOGGER.error("getChildren failed",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }, null);
    }
}
