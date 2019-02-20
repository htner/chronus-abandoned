package main

import (
	"net/http"
	"fmt"
	"os"
	"flag"
    "encoding/json"
    "strings"
    "github.com/coreos/etcd/raft"
    "github.com/angopher/chronus/raftmeta"
    "github.com/angopher/chronus/raftmeta/internal"
    "github.com/angopher/chronus/x"
    "github.com/angopher/chronus/services/meta"
    "github.com/BurntSushi/toml"
)

var metaService *raftmeta.MetaService

func main() {
	configFile := flag.String("config", "", "-config config_file")
	flag.Parse()

	config := raftmeta.NewConfig()
	if *configFile != "" {
		x.Check((&config).FromTomlFile(*configFile))
	} else {
		toml.NewEncoder(os.Stdout).Encode(&config)
		return
	}

	fmt.Printf("config:%+v\n", config)

    c := &raft.Config{
        ID:              config.RaftId,
        ElectionTick:    config.ElectionTick,
        HeartbeatTick:   config.HeartbeatTick,
        MaxSizePerMsg:   config.MaxSizePerMsg,
        MaxInflightMsgs: config.MaxInflightMsgs,
    }
	//peers := []raft.Peer{{ID: uint64(*selfID)}, {ID: uint64(*id2)}, {ID: uint64(*id3)}}
	peers := []raft.Peer{}
	for _, p := range config.Peers {
		rc := internal.RaftContext{Addr: p.Addr, ID: p.RaftId}
		data, err := json.Marshal(&rc)
		x.Check(err)
        peers = append(peers, raft.Peer{ID: p.RaftId, Context: data})
	}

	metaCli := meta.NewClient(&meta.Config{
		RetentionAutoCreate: config.RetentionAutoCreate,
		LoggingEnabled:true,
	})
	err := metaCli.Open()
	x.Check(err)

	node := raftmeta.NewRaftNode(c, config)
	node.MetaCli = metaCli

    node.InitAndStartNode(peers)
    go node.Run()
    linearRead := raftmeta.NewLinearizabler(node)
    go linearRead.ReadLoop()

	metaService = raftmeta.NewMetaService(metaCli, node, linearRead)


    http.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
		node.HandleMessage(w, r)
	})
    http.HandleFunc("/update_cluster", func(w http.ResponseWriter, r *http.Request) {
		node.HandleUpdateCluster(w, r)
    })

    initHttpHandler()

    ipPort := strings.Split(config.MyAddr, ":")
    listenAddr := ":" + ipPort[1]
	fmt.Println("listen:", listenAddr)
	err = http.ListenAndServe(listenAddr, nil)
    if err != nil {
        fmt.Printf("msg=ListenAndServe failed,err=%s\n", err.Error())
    }
}

func initHttpHandler() {
    http.HandleFunc(raftmeta.DATA_PATH, metaService.Data)
    http.HandleFunc(raftmeta.CREATE_DATABASE_PATH, metaService.CreateDatabase)
    http.HandleFunc(raftmeta.DROP_DATABASE_PATH, metaService.DropDatabase)
    http.HandleFunc(raftmeta.CREATE_SHARD_GROUP_PATH, metaService.CreateShardGroup)
    http.HandleFunc(raftmeta.CREATE_DATA_NODE_PATH, metaService.CreateDataNode)

    http.HandleFunc(raftmeta.DROP_RETENTION_POLICY_PATH, metaService.DropRetentionPolicy)
    http.HandleFunc(raftmeta.DELETE_DATA_NODE_PATH, metaService.DeleteDataNode)
    http.HandleFunc(raftmeta.CREATE_RETENTION_POLICY_PATH, metaService.CreateRetentionPolicy)
    http.HandleFunc(raftmeta.UPDATE_RETENTION_POLICY_PATH, metaService.UpdateRetentionPolicy)
    http.HandleFunc(raftmeta.CREATE_USER_PATH, metaService.CreateUser)
    http.HandleFunc(raftmeta.DROP_USER_PATH, metaService.DropUser)
    http.HandleFunc(raftmeta.UPDATE_USER_PATH, metaService.UpdateUser)
    http.HandleFunc(raftmeta.SET_PRIVILEGE_PATH, metaService.SetPrivilege)
    http.HandleFunc(raftmeta.SET_ADMIN_PRIVILEGE, metaService.SetAdminPrivilege)
    http.HandleFunc(raftmeta.AUTHENTICATE_PATH, metaService.Authenticate)
    http.HandleFunc(raftmeta.DROP_SHARD_PATH, metaService.DropShard)
    http.HandleFunc(raftmeta.TRUNCATE_SHARD_GROUPS_PATH, metaService.TruncateShardGroups)
    http.HandleFunc(raftmeta.PRUNE_SHARD_GROUPS_PATH, metaService.PruneShardGroups)
    http.HandleFunc(raftmeta.DELETE_SHARD_GROUP_PATH, metaService.DeleteShardGroup)
    http.HandleFunc(raftmeta.PRECREATE_SHARD_GROUPS_PATH, metaService.PrecreateShardGroups)
    http.HandleFunc(raftmeta.CREATE_DATABASE_WITH_RETENTION_POLICY_PATH, metaService.CreateDatabaseWithRetentionPolicy)
    http.HandleFunc(raftmeta.CREATE_CONTINUOUS_QUERY_PATH, metaService.CreateContinuousQuery)
    http.HandleFunc(raftmeta.DROP_CONTINUOUS_QUERY_PATH, metaService.DropContinuousQuery)
    http.HandleFunc(raftmeta.CREATE_SUBSCRIPTION_PATH, metaService.CreateSubscription)
    http.HandleFunc(raftmeta.DROP_SUBSCRIPTION_PATH, metaService.DropSubscription)
    http.HandleFunc(raftmeta.ACQUIRE_LEASE_PATH, metaService.AcquireLease)
    http.HandleFunc(raftmeta.PING_PATH, metaService.Ping)
}

