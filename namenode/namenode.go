package namenode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	datanode "github.com/Raghav-Tiruvallur/GoDFS/proto/datanode"
	namenode "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type DataNodeMetadata struct {
	ID            string
	Port          string
	Status        string
	LastHeartbeat time.Time
}
type NameNodeData struct {
	BlockSize                 int64
	DataNodeToBlockMapping    map[string][]string
	ReplicationFactor         int64
	DataNodeToMetadataMapping map[string]DataNodeMetadata
	FileToBlockMapping        map[string][]string
	BlockChecksumMapping      map[string]string
	mutex                     sync.RWMutex
	namenode.UnimplementedNamenodeServiceServer
}

type DataNodeBlockCount struct {
	DataNodeData *namenode.DatanodeData
	BlockCount   int64
}

const (
	heartbeatTimeout = 30 * time.Second
	monitorInterval  = 10 * time.Second
)

func (nameNode *NameNodeData) InitializeNameNode(port string, blockSize int64) {

	nameNode.BlockSize = blockSize
	nameNode.DataNodeToBlockMapping = make(map[string][]string)
	nameNode.DataNodeToMetadataMapping = make(map[string]DataNodeMetadata)
	nameNode.FileToBlockMapping = make(map[string][]string)
	nameNode.BlockChecksumMapping = make(map[string]string)
	nameNode.ReplicationFactor = 3
	go nameNode.StartReplicationMonitor()
	server := grpc.NewServer()
	namenode.RegisterNamenodeServiceServer(server, nameNode)
	address := ":" + port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Namenode is listening on port %s\n", address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (nameNode *NameNodeData) Register_DataNode(ctx context.Context, datanodeData *namenode.DatanodeData) (status *namenode.Status, err error) {

	log.Printf("%s %d\n", datanodeData.DatanodeID, nameNode.BlockSize)
	nameNode.mutex.Lock()
	defer nameNode.mutex.Unlock()
	_, ok := nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID]
	if !ok {
		nameNode.DataNodeToBlockMapping[datanodeData.DatanodeID] = make([]string, 0)
		dnmetadata := DataNodeMetadata{
			ID:            datanodeData.DatanodeID,
			Port:          datanodeData.DatanodePort,
			Status:        "Available",
			LastHeartbeat: time.Now(),
		}
		nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = dnmetadata
		return &namenode.Status{StatusMessage: "Registered"}, nil
	}
	existing := nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID]
	existing.Status = "Available"
	existing.LastHeartbeat = time.Now()
	nameNode.DataNodeToMetadataMapping[datanodeData.DatanodeID] = existing
	return &namenode.Status{StatusMessage: "Exists"}, nil

}

func (nameNode *NameNodeData) GetAvailableDatanodes(ctx context.Context, empty *empty.Empty) (freeNodes *namenode.FreeDataNodes, err error) {
	nameNode.mutex.RLock()
	defer nameNode.mutex.RUnlock()
	availableDataNodes := make([]*DataNodeBlockCount, 0)
	freeDataNodes := make([]*namenode.DatanodeData, 0)
	for dataNodeID, datanodeMetadata := range nameNode.DataNodeToMetadataMapping {
		if datanodeMetadata.Status == "Available" {
			datanodeData := &namenode.DatanodeData{DatanodeID: dataNodeID, DatanodePort: nameNode.DataNodeToMetadataMapping[dataNodeID].Port}
			blockCount := int64(len(nameNode.DataNodeToBlockMapping[dataNodeID]))
			dataNodeBlockCount := &DataNodeBlockCount{DataNodeData: datanodeData, BlockCount: blockCount}
			availableDataNodes = append(availableDataNodes, dataNodeBlockCount)
		}
	}

	sort.SliceStable(availableDataNodes, func(i, j int) bool {
		return availableDataNodes[i].BlockCount < availableDataNodes[j].BlockCount
	})

	availableCount := len(availableDataNodes)
	requiredCount := int(nameNode.ReplicationFactor)
	if availableCount < requiredCount {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"insufficient datanodes: need %d, available %d",
			requiredCount,
			availableCount,
		)
	}

	for i := 0; i < int(nameNode.ReplicationFactor); i++ {
		freeDataNode := &namenode.DatanodeData{DatanodeID: availableDataNodes[i].DataNodeData.DatanodeID, DatanodePort: availableDataNodes[i].DataNodeData.DatanodePort}
		freeDataNodes = append(freeDataNodes, freeDataNode)
	}
	return &namenode.FreeDataNodes{DataNodeIDs: freeDataNodes[:nameNode.ReplicationFactor]}, nil

}

func (nameNode *NameNodeData) BlockReport(ctx context.Context, dataNodeBlockData *namenode.DatanodeBlockData) (status *namenode.Status, err error) {
	nameNode.mutex.Lock()
	defer nameNode.mutex.Unlock()
	nameNode.DataNodeToBlockMapping[dataNodeBlockData.DatanodeID] = dataNodeBlockData.Blocks
	if metadata, ok := nameNode.DataNodeToMetadataMapping[dataNodeBlockData.DatanodeID]; ok {
		metadata.Status = "Available"
		metadata.LastHeartbeat = time.Now()
		nameNode.DataNodeToMetadataMapping[dataNodeBlockData.DatanodeID] = metadata
	}
	return &namenode.Status{StatusMessage: "Block Report Recieved"}, nil
}

func (nameNode *NameNodeData) FindDataNodesByBlock(blockID string) []DataNodeMetadata {
	nameNode.mutex.RLock()
	defer nameNode.mutex.RUnlock()
	dataNodes := make([]DataNodeMetadata, 0)

	for dataNode, blocks := range nameNode.DataNodeToBlockMapping {
		if utils.ValueInArray(blockID, blocks) && nameNode.DataNodeToMetadataMapping[dataNode].Status == "Available" {
			dataNodes = append(dataNodes, nameNode.DataNodeToMetadataMapping[dataNode])
		}

	}
	return dataNodes
}

func (nameNode *NameNodeData) GetDataNodesForFile(ctx context.Context, fileData *namenode.FileData) (*namenode.BlockData, error) {
	nameNode.mutex.RLock()
	blocks, ok := nameNode.FileToBlockMapping[fileData.FileName]
	nameNode.mutex.RUnlock()
	dataNodes := make([]*namenode.BlockDataNode, 0)
	if !ok {
		return nil, errors.New("file does not exist")
	}
	for _, block := range blocks {
		dataNodeList := nameNode.FindDataNodesByBlock(block)
		dataNodeIDsList := make([]*namenode.DatanodeData, 0)
		for _, datanode := range dataNodeList {
			dataNodeIDsList = append(dataNodeIDsList, &namenode.DatanodeData{DatanodeID: datanode.ID, DatanodePort: datanode.Port})
		}
		nameNode.mutex.RLock()
		blockData := &namenode.BlockDataNode{BlockID: block, DataNodeIDs: dataNodeIDsList, Checksum: nameNode.BlockChecksumMapping[block]}
		nameNode.mutex.RUnlock()
		dataNodes = append(dataNodes, blockData)
	}

	return &namenode.BlockData{BlockDataNodes: dataNodes}, nil

}

func (nameNode *NameNodeData) FileBlockMapping(ctx context.Context, fileBlockMetadata *namenode.FileBlockMetadata) (*namenode.Status, error) {
	nameNode.mutex.Lock()
	defer nameNode.mutex.Unlock()
	filePath := fileBlockMetadata.FilePath
	blockIDs := fileBlockMetadata.BlockIDs

	if len(fileBlockMetadata.Blocks) > 0 {
		blockIDs = make([]string, 0, len(fileBlockMetadata.Blocks))
		for _, block := range fileBlockMetadata.Blocks {
			blockIDs = append(blockIDs, block.BlockID)
			nameNode.BlockChecksumMapping[block.BlockID] = block.Checksum
		}
	}

	nameNode.FileToBlockMapping[filePath] = blockIDs
	return &namenode.Status{StatusMessage: "Success"}, nil

}

func (nameNode *NameNodeData) StartReplicationMonitor() {
	ticker := time.NewTicker(monitorInterval)
	for range ticker.C {
		nameNode.markTimedOutDataNodes()
		nameNode.reReplicateUnderReplicatedBlocks()
	}
}

func (nameNode *NameNodeData) markTimedOutDataNodes() {
	nameNode.mutex.Lock()
	defer nameNode.mutex.Unlock()
	now := time.Now()
	for dataNodeID, metadata := range nameNode.DataNodeToMetadataMapping {
		if metadata.Status == "Available" && now.Sub(metadata.LastHeartbeat) > heartbeatTimeout {
			metadata.Status = "Dead"
			nameNode.DataNodeToMetadataMapping[dataNodeID] = metadata
			log.Printf("Marked datanode %s as Dead (heartbeat timeout)", dataNodeID)
		}
	}
}

type replicationTask struct {
	blockID       string
	sourcePort    string
	targetID      string
	targetPort    string
	sourceNodeID  string
}

func (nameNode *NameNodeData) reReplicateUnderReplicatedBlocks() {
	tasks := make([]replicationTask, 0)
	nameNode.mutex.RLock()
	for blockID := range nameNode.BlockChecksumMapping {
		healthyNodes := make([]DataNodeMetadata, 0)
		replicaNodeIDs := make(map[string]struct{})
		for nodeID, blocks := range nameNode.DataNodeToBlockMapping {
			if !utils.ValueInArray(blockID, blocks) {
				continue
			}
			replicaNodeIDs[nodeID] = struct{}{}
			metadata := nameNode.DataNodeToMetadataMapping[nodeID]
			if metadata.Status == "Available" {
				healthyNodes = append(healthyNodes, metadata)
			}
		}
		if len(healthyNodes) == 0 || len(healthyNodes) >= int(nameNode.ReplicationFactor) {
			continue
		}
		source := healthyNodes[0]
		for nodeID, metadata := range nameNode.DataNodeToMetadataMapping {
			if metadata.Status != "Available" {
				continue
			}
			if _, alreadyHasBlock := replicaNodeIDs[nodeID]; alreadyHasBlock {
				continue
			}
			tasks = append(tasks, replicationTask{
				blockID:      blockID,
				sourcePort:   source.Port,
				targetID:     metadata.ID,
				targetPort:   metadata.Port,
				sourceNodeID: source.ID,
			})
			healthyNodes = append(healthyNodes, metadata)
			replicaNodeIDs[nodeID] = struct{}{}
			if len(healthyNodes) >= int(nameNode.ReplicationFactor) {
				break
			}
		}
	}
	nameNode.mutex.RUnlock()

	for _, task := range tasks {
		if err := nameNode.replicateBlock(task); err != nil {
			log.Printf("re-replication failed for block %s: %v", task.blockID, err)
		}
	}
}

func (nameNode *NameNodeData) replicateBlock(task replicationTask) error {
	sourceConn, err := grpc.Dial(net.JoinHostPort("localhost", task.sourcePort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("source dial failed: %w", err)
	}
	defer sourceConn.Close()
	sourceClient := datanode.NewDatanodeServiceClient(sourceConn)
	blockResponse, err := sourceClient.ReadBytesFromDataNode(context.Background(), &datanode.BlockRequest{BlockID: task.blockID})
	if err != nil {
		return fmt.Errorf("source read failed from %s: %w", task.sourceNodeID, err)
	}

	targetConn, err := grpc.Dial(net.JoinHostPort("localhost", task.targetPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("target dial failed: %w", err)
	}
	defer targetConn.Close()
	targetClient := datanode.NewDatanodeServiceClient(targetConn)
	_, err = targetClient.SendDataToDataNodes(context.Background(), &datanode.ClientToDataNodeRequest{BlockID: task.blockID, Content: blockResponse.FileContent})
	if err != nil {
		return fmt.Errorf("target write failed: %w", err)
	}

	nameNode.mutex.Lock()
	defer nameNode.mutex.Unlock()
	if !utils.ValueInArray(task.blockID, nameNode.DataNodeToBlockMapping[task.targetID]) {
		nameNode.DataNodeToBlockMapping[task.targetID] = append(nameNode.DataNodeToBlockMapping[task.targetID], task.blockID)
	}
	log.Printf("Re-replicated block %s from %s to %s", task.blockID, task.sourceNodeID, task.targetID)
	return nil
}
