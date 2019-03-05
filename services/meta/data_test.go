package meta_test

import (
	"testing"

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
	//TODO
}

func TestDeleteDataNode(t *testing.T) {
	//TODO
}
