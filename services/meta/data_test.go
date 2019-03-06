package meta_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"

	imeta "github.com/angopher/chronus/services/meta"
)

func newData() *imeta.Data {
	return &imeta.Data{
		Data: meta.Data{
			Index:     1,
			ClusterID: 0,
		},
	}
}

func TestCreateDataNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	if err := data.CreateDataNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	expN := meta.NodeInfo{ID: 1, TCPHost: tcpHost, Host: host}
	n := data.DataNode(1)
	if n.ID != expN.ID || n.TCPHost != expN.TCPHost || n.Host != expN.Host {
		t.Fatalf("expected node: %+v, got: %+v", expN, n)
	}

	if err, exp := data.CreateDataNode(host, tcpHost), imeta.ErrNodeExists; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}
}

func TestCreateAndDeleteMetaNode(t *testing.T) {
	data := newData()
	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	if err := data.CreateMetaNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	expN := meta.NodeInfo{ID: 1, TCPHost: tcpHost, Host: host}
	n := data.MetaNodes[0]
	if n.ID != expN.ID || n.TCPHost != expN.TCPHost || n.Host != expN.Host {
		t.Fatalf("expected node: %+v, got: %+v", expN, n)
	}

	if err, exp := data.CreateMetaNode(host, tcpHost), imeta.ErrNodeExists; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err, exp := data.DeleteMetaNode(0), imeta.ErrNodeIDRequired; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err, exp := data.DeleteMetaNode(2), imeta.ErrNodeNotFound; err != exp {
		t.Fatalf("expected err: %s, got: %s", exp, err)
	}

	if err := data.DeleteMetaNode(expN.ID); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
}

func TestCreateShardGroup(t *testing.T) {
	data := newData()
	name := "testdb"
	policy := meta.DefaultRetentionPolicyName
	data.CreateDatabase(name)
	data.CreateRetentionPolicy(name, meta.DefaultRetentionPolicyInfo(), true)

	if err := data.CreateShardGroup(name, policy, time.Now()); err != imeta.ErrNodeNotFound {
		t.Fatalf("expected err: %s, got: %s", imeta.ErrNodeNotFound, err)
	}

	host := "127.0.0.1:8080"
	tcpHost := "127.0.0.1:8081"
	if err := data.CreateDataNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	host = "127.0.0.1:9080"
	tcpHost = "127.0.0.1:9081"
	if err := data.CreateDataNode(host, tcpHost); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if err := data.CreateShardGroup(name, policy, time.Now()); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	rp := data.Database(name).RetentionPolicy(policy)
	sgi := rp.ShardGroups[0]
	if len(sgi.Shards) != 2 {
		t.Fatalf("unexpected 2 shards, got: %d", len(sgi.Shards))
	}

	for _, sh := range sgi.Shards {
		o := sh.Owners[0]
		if len(sh.Owners) != 1 || o.NodeID != 1 && o.NodeID != 2 {
			t.Fatalf("unexpected shard: %+v", sh)
		}
	}

	//TODO: replicaN>1
}

func TestDeleteDataNode(t *testing.T) {
	//TODO
}
