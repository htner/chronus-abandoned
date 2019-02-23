package meta

import (
	"sort"
	"time"

	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/services/meta"

    internal "github.com/angopher/chronus/services/meta/internal"
)

//go:generate protoc --gogo_out=. internal/meta.proto

// Data represents the top level collection of all metadata.
type Data struct {
    meta.Data
	MetaNodes []meta.NodeInfo
	DataNodes []meta.NodeInfo

	MaxNodeID       uint64
}

// DataNode returns a node by id.
func (data *Data) DataNode(id uint64) *meta.NodeInfo {
	for i := range data.DataNodes {
		if data.DataNodes[i].ID == id {
			return &data.DataNodes[i]
		}
	}
	return nil
}

// CreateDataNode adds a node to the metadata.
func (data *Data) CreateDataNode(host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpHost {
			return ErrNodeExists
		}
	}

	// If an existing meta node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.MetaNodes {
		if n.TCPHost == tcpHost {
			existingID = n.ID
			break
		}
	}

	// We didn't find an existing node, so assign it a new node ID
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, meta.NodeInfo{
		ID:      existingID,
		Host:    host,
		TCPHost: tcpHost,
	})
	sort.Sort(meta.NodeInfos(data.DataNodes))

	return nil
}

// setDataNode adds a data node with a pre-specified nodeID.
// this should only be used when the cluster is upgrading from 0.9 to 0.10
func (data *Data) setDataNode(nodeID uint64, host, tcpHost string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.DataNodes {
		if n.Host == host {
			return ErrNodeExists
		}
	}

	// Append new node.
	data.DataNodes = append(data.DataNodes, meta.NodeInfo{
		ID:      nodeID,
		Host:    host,
		TCPHost: tcpHost,
	})

	return nil
}

// DeleteDataNode removes a node from the Meta store.
//
// If necessary, DeleteDataNode reassigns ownership of any shards that
// would otherwise become orphaned by the removal of the node from the
// cluster.
func (data *Data) DeleteDataNode(id uint64) error {
	var nodes []meta.NodeInfo

	// Remove the data node from the store's list.
	for _, n := range data.DataNodes {
		if n.ID != id {
			nodes = append(nodes, n)
		}
	}

	if len(nodes) == len(data.DataNodes) {
		return ErrNodeNotFound
	}
	data.DataNodes = nodes

	// Remove node id from all shard infos
	for di, d := range data.Databases {
		for ri, rp := range d.RetentionPolicies {
			for sgi, sg := range rp.ShardGroups {
				var (
					nodeOwnerFreqs = make(map[int]int)
					orphanedShards []meta.ShardInfo
				)
				// Look through all shards in the shard group and
				// determine (1) if a shard no longer has any owners
				// (orphaned); (2) if all shards in the shard group
				// are orphaned; and (3) the number of shards in this
				// group owned by each data node in the cluster.
				for si, s := range sg.Shards {
					// Track of how many shards in the group are
					// owned by each data node in the cluster.
					var nodeIdx = -1
					for i, owner := range s.Owners {
						if owner.NodeID == id {
							nodeIdx = i
						}
						nodeOwnerFreqs[int(owner.NodeID)]++
					}

					if nodeIdx > -1 {
						// Data node owns shard, so relinquish ownership
						// and set new owners on the shard.
						s.Owners = append(s.Owners[:nodeIdx], s.Owners[nodeIdx+1:]...)
						data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards[si].Owners = s.Owners
					}

					// Shard no longer owned. Will need reassigning
					// an owner.
					if len(s.Owners) == 0 {
						orphanedShards = append(orphanedShards, s)
					}
				}

				// Mark the shard group as deleted if it has no shards,
				// or all of its shards are orphaned.
				if len(sg.Shards) == 0 || len(orphanedShards) == len(sg.Shards) {
					data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].DeletedAt = time.Now().UTC()
					continue
				}

				// Reassign any orphaned shards. Delete the node we're
				// dropping from the list of potential new owners.
				delete(nodeOwnerFreqs, int(id))

				for _, orphan := range orphanedShards {
					newOwnerID, err := newShardOwner(orphan, nodeOwnerFreqs)
					if err != nil {
						return err
					}

					for si, s := range sg.Shards {
						if s.ID == orphan.ID {
							sg.Shards[si].Owners = append(sg.Shards[si].Owners, meta.ShardOwner{NodeID: newOwnerID})
							data.Databases[di].RetentionPolicies[ri].ShardGroups[sgi].Shards = sg.Shards
							break
						}
					}

				}
			}
		}
	}
	return nil
}

func (data *Data) CloneNodes(src []meta.NodeInfo) []meta.NodeInfo {
	if len(src) == 0 {
		return []meta.NodeInfo{}
	}
	nodes := make([]meta.NodeInfo, len(src))
	for i := range src {
		nodes[i] = src[i]
	}

	return nodes
}

// newShardOwner sets the owner of the provided shard to the data node
// that currently owns the fewest number of shards. If multiple nodes
// own the same (fewest) number of shards, then one of those nodes
// becomes the new shard owner.
func newShardOwner(s meta.ShardInfo, ownerFreqs map[int]int) (uint64, error) {
	var (
		minId   = -1
		minFreq int
	)

	for id, freq := range ownerFreqs {
		if minId == -1 || freq < minFreq {
			minId, minFreq = int(id), freq
		}
	}

	if minId < 0 {
		return 0, fmt.Errorf("cannot reassign shard %d due to lack of data nodes", s.ID)
	}

	// Update the shard owner frequencies and set the new owner on the
	// shard.
	ownerFreqs[minId]++
	return uint64(minId), nil
}

// MetaNode returns a node by id.
func (data *Data) MetaNode(id uint64) *meta.NodeInfo {
	for i := range data.MetaNodes {
		if data.MetaNodes[i].ID == id {
			return &data.MetaNodes[i]
		}
	}
	return nil
}

// CreateMetaNode will add a new meta node to the metastore
func (data *Data) CreateMetaNode(httpAddr, tcpAddr string) error {
	// Ensure a node with the same host doesn't already exist.
	for _, n := range data.MetaNodes {
		if n.Host == httpAddr {
			return ErrNodeExists
		}
	}

	// If an existing data node exists with the same TCPHost address,
	// then these nodes are actually the same so re-use the existing ID
	var existingID uint64
	for _, n := range data.DataNodes {
		if n.TCPHost == tcpAddr {
			existingID = n.ID
			break
		}
	}

	// We didn't find and existing data node ID, so assign a new ID
	// to this meta node.
	if existingID == 0 {
		data.MaxNodeID++
		existingID = data.MaxNodeID
	}

	// Append new node.
	data.MetaNodes = append(data.MetaNodes, meta.NodeInfo{
		ID:      existingID,
		Host:    httpAddr,
		TCPHost: tcpAddr,
	})

	sort.Sort(meta.NodeInfos(data.MetaNodes))
	return nil
}

// SetMetaNode will update the information for the single meta
// node or create a new metanode. If there are more than 1 meta
// nodes already, an error will be returned
func (data *Data) SetMetaNode(httpAddr, tcpAddr string) error {
	if len(data.MetaNodes) > 1 {
		return fmt.Errorf("can't set meta node when there are more than 1 in the metastore")
	}

	if len(data.MetaNodes) == 0 {
		return data.CreateMetaNode(httpAddr, tcpAddr)
	}

	data.MetaNodes[0].Host = httpAddr
	data.MetaNodes[0].TCPHost = tcpAddr

	return nil
}

// DeleteMetaNode will remove the meta node from the store
func (data *Data) DeleteMetaNode(id uint64) error {
	// Node has to be larger than 0 to be real
	if id == 0 {
		return ErrNodeIDRequired
	}

	var nodes []meta.NodeInfo
	for _, n := range data.MetaNodes {
		if n.ID == id {
			continue
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == len(data.MetaNodes) {
		return ErrNodeNotFound
	}

	data.MetaNodes = nodes
	return nil
}

// Clone returns a copy of data with a new version.
func (data *Data) Clone() *Data {
	other := *data

	other.Databases = data.CloneDatabases()
	other.Users = data.CloneUsers()
	other.DataNodes = data.CloneNodes(data.DataNodes)
	other.MetaNodes = data.CloneNodes(data.MetaNodes)

	return &other
}

func MarshalNodeInfo(ni meta.NodeInfo) *internal.NodeInfo {
    pb := &internal.NodeInfo{}
    pb.ID = proto.Uint64(ni.ID)
    pb.Host = proto.String(ni.Host)
    pb.TCPHost = proto.String(ni.TCPHost)
    return pb
}

// unmarshal deserializes from a protobuf representation.
func UnmarshalNodeInfo(ni *meta.NodeInfo, pb *internal.NodeInfo) {
    ni.ID = pb.GetID()
    ni.Host = pb.GetHost()
    ni.TCPHost = pb.GetTCPHost()
}

func (data *Data) marshal() *internal.Data {
    pb := &internal.Data{
        Term:      proto.Uint64(data.Term),
        Index:     proto.Uint64(data.Index),
        ClusterID: proto.Uint64(data.ClusterID),

		MaxShardGroupID: proto.Uint64(data.MaxShardGroupID),
		MaxShardID:      proto.Uint64(data.MaxShardID),

		// Need this for reverse compatibility
		MaxNodeID: proto.Uint64(data.MaxNodeID),
	}

	pb.DataNodes = make([]*internal.NodeInfo, len(data.DataNodes))
	for i := range data.DataNodes {
		pb.DataNodes[i] = MarshalNodeInfo(data.DataNodes[i])
	}

	pb.MetaNodes = make([]*internal.NodeInfo, len(data.MetaNodes))
	for i := range data.MetaNodes {
		pb.MetaNodes[i] = MarshalNodeInfo(data.MetaNodes[i])
	}

    //TODO:
//	pb.Databases = make([]*internal.DatabaseInfo, len(data.Databases))
//	for i := range data.Databases {
//		pb.Databases[i] = data.Databases[i].marshal()
//	}
//
//	pb.Users = make([]*internal.UserInfo, len(data.Users))
//	for i := range data.Users {
//		pb.Users[i] = data.Users[i].marshal()
//	}

	return pb
}

// unmarshal deserializes from a protobuf representation.
func (data *Data) unmarshal(pb *internal.Data) {
	data.Term = pb.GetTerm()
	data.Index = pb.GetIndex()
	data.ClusterID = pb.GetClusterID()

	data.MaxShardGroupID = pb.GetMaxShardGroupID()
	data.MaxShardID = pb.GetMaxShardID()
	data.MaxNodeID = pb.GetMaxNodeID()

	data.DataNodes = make([]meta.NodeInfo, len(pb.GetDataNodes()))
	for i, x := range pb.GetDataNodes() {
        UnmarshalNodeInfo(&data.DataNodes[i], x)
	}

	data.MetaNodes = make([]meta.NodeInfo, len(pb.GetMetaNodes()))
	for i, x := range pb.GetMetaNodes() {
        UnmarshalNodeInfo(&data.MetaNodes[i], x)
	}

    //TODO
//	data.Databases = make([]meta.DatabaseInfo, len(pb.GetDatabases()))
//	for i, x := range pb.GetDatabases() {
//		data.Databases[i].unmarshal(x)
//	}
//
//	data.Users = make([]meta.UserInfo, len(pb.GetUsers()))
//	for i, x := range pb.GetUsers() {
//		data.Users[i].unmarshal(x)
//	}
//
//	// Exhaustively determine if there is an admin user. The marshalled cache
//	// value may not be correct.
//	data.adminUserExists = data.hasAdminUser()
}

// MarshalBinary encodes the metadata to a binary format.
func (data *Data) MarshalBinary() ([]byte, error) {
    //TODO
	return proto.Marshal(data.marshal())
}

// UnmarshalBinary decodes the object from a binary format.
func (data *Data) UnmarshalBinary(buf []byte) error {
    //TODO
	var pb internal.Data
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	data.unmarshal(&pb)
	return nil
}

