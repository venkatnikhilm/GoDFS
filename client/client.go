package client

import (
	"crypto/sha256"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"

	datanodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/datanode"
	namenodeService "github.com/Raghav-Tiruvallur/GoDFS/proto/namenode"
	"github.com/Raghav-Tiruvallur/GoDFS/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientData struct {
	NameNodePort string
	Port         string
}

type Block struct {
	blockID string
}

type Pair[T any, V any] struct {
	first  T
	second V
}

type BlockUploadResult struct {
	BlockID  string
	Checksum string
}

func (client *ClientData) InitializeClient(nameNodePort string) {
	client.NameNodePort = nameNodePort
}
func (client *ClientData) ConnectToNameNode() *grpc.ClientConn {

	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return conn

}

func GetDataNodeStub(port string) datanodeService.DatanodeServiceClient {
	connectionString := net.JoinHostPort("localhost", port)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dataNodeClient := datanodeService.NewDatanodeServiceClient(conn)
	return dataNodeClient
}

func (client *ClientData) GetNameNodeStub() namenodeService.NamenodeServiceClient {
	connectionString := net.JoinHostPort("localhost", client.NameNodePort)
	conn, _ := grpc.Dial(connectionString, grpc.WithTransportCredentials(insecure.NewCredentials()))
	nameNodeClient := namenodeService.NewNamenodeServiceClient(conn)
	return nameNodeClient
}

func (client *ClientData) GetAvailableDatanodes(conn *grpc.ClientConn) (*namenodeService.FreeDataNodes, error) {

	namenodeClient := namenodeService.NewNamenodeServiceClient(conn)
	freeDataNodes, err := namenodeClient.GetAvailableDatanodes(context.Background(), &emptypb.Empty{})
	return freeDataNodes, err

}

func ThreadDone(done chan Pair[int, BlockUploadResult], blockID string, checksum string, idx int) {

	done <- Pair[int, BlockUploadResult]{first: idx, second: BlockUploadResult{BlockID: blockID, Checksum: checksum}}
}
func SendData(dataNodeID string, datanodePort string, blockID string, buffer []byte, n int) error {
	clientDataNodeRequest := &datanodeService.ClientToDataNodeRequest{BlockID: blockID, Content: buffer[:n]}
	datanodeClient := GetDataNodeStub(datanodePort)
	_, err := datanodeClient.SendDataToDataNodes(context.Background(), clientDataNodeRequest)
	if err != nil {
		return fmt.Errorf("failed to send block %s to datanode %s: %w", blockID, dataNodeID, err)
	}
	return nil

}

func (client *ClientData) ProcessData(conn *grpc.ClientConn, blockSize int, done chan Pair[int, BlockUploadResult], filePath string, start int, idx int) {

	blockID := uuid.New().String()

	fileHandler, err := os.Open(filePath)

	utils.ErrorHandler(err)
	fileInfo, err := fileHandler.Stat()
	fileSize := fileInfo.Size()
	end := start + blockSize
	if end > int(fileSize) {
		end = int(fileSize)
	}
	readBytes := end - start
	buffer := make([]byte, readBytes)
	n, err := fileHandler.ReadAt(buffer, int64(start))
	if err == io.EOF {
		return
	}
	utils.ErrorHandler(err)
	checksum := sha256.Sum256(buffer[:n])
	checksumHex := hex.EncodeToString(checksum[:])
	freeDataNodes, err := client.GetAvailableDatanodes(conn)
	if err != nil {
		log.Printf("failed to fetch available datanodes for block %d: %v", idx, err)
		return
	}
	wg2 := &sync.WaitGroup{}
	replicationErrCh := make(chan error, len(freeDataNodes.DataNodeIDs))
	for _, datanode := range freeDataNodes.DataNodeIDs {
		wg2.Add(1)
		go func(datanode *namenodeService.DatanodeData) {
			defer wg2.Done()
			if err := SendData(datanode.DatanodeID, datanode.DatanodePort, blockID, buffer, n); err != nil {
				replicationErrCh <- err
			}
		}(datanode)
	}
	wg2.Wait()
	close(replicationErrCh)

	for replicationErr := range replicationErrCh {
		log.Printf("block replication failed for block %s (index %d): %v", blockID, idx, replicationErr)
		return
	}

	ThreadDone(done, blockID, checksumHex, idx)
}

func (client *ClientData) SendFileBlockMappingToNameNode(filePath string, blockData []BlockUploadResult) {

	nameNodeStub := client.GetNameNodeStub()
	blockIDs := make([]string, 0, len(blockData))
	blocks := make([]*namenodeService.BlockMetadata, 0, len(blockData))
	for _, block := range blockData {
		blockIDs = append(blockIDs, block.BlockID)
		blocks = append(blocks, &namenodeService.BlockMetadata{BlockID: block.BlockID, Checksum: block.Checksum})
	}
	fileBlockMetadata := &namenodeService.FileBlockMetadata{FilePath: filePath, BlockIDs: blockIDs, Blocks: blocks}
	status, err := nameNodeStub.FileBlockMapping(context.Background(), fileBlockMetadata)
	utils.ErrorHandler(err)
	log.Println("Sent file block mapping to namenode with status:", status.StatusMessage)
}

func (client *ClientData) WriteFile(conn *grpc.ClientConn, sourcePath string, fileName string) {

	filePath := filepath.Join(sourcePath, fileName)
	freeDataNodes, err := client.GetAvailableDatanodes(conn)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
			log.Printf("write aborted: %s", st.Message())
			return
		}
		utils.ErrorHandler(err)
	}
	if len(freeDataNodes.DataNodeIDs) == 0 {
		log.Println("write aborted: no datanodes available")
		return
	}

	blockSize := int(3 * 1024)
	fileSizeHandler, err := os.Stat(filePath)

	utils.ErrorHandler(err)

	fileSize := int(fileSizeHandler.Size())

	numberOfBlocks := fileSize / blockSize

	if fileSize%blockSize > 0 {
		numberOfBlocks++
	}

	done := make(chan Pair[int, BlockUploadResult])
	startList := make([]int64, 0)

	amount := 0

	for {
		if amount > fileSize {
			break
		}
		startList = append(startList, int64(amount))
		amount += blockSize

	}
	wg1 := &sync.WaitGroup{}
	for i := 0; i < numberOfBlocks; i++ {
		wg1.Add(1)
		go func(start int, idx int) {
			defer wg1.Done()
			client.ProcessData(conn, blockSize, done, filePath, start, idx)
		}(int(startList[i]), i)
	}
	go func() {
		wg1.Wait()
		close(done)
	}()

	sortedblockIDs := make([]Pair[int, BlockUploadResult], 0)
	for i := 0; i < numberOfBlocks; i++ {
		block, ok := <-done
		if !ok {
			break
		}
		sortedblockIDs = append(sortedblockIDs, block)
	}
	if len(sortedblockIDs) != numberOfBlocks {
		utils.ErrorHandler(errors.New("write failed: one or more blocks were not uploaded"))
	}
	sort.Slice(sortedblockIDs, func(i, j int) bool {
		return sortedblockIDs[i].first < sortedblockIDs[j].first
	})
	blockData := make([]BlockUploadResult, 0)
	for _, block := range sortedblockIDs {
		blockData = append(blockData, block.second)
	}
	client.SendFileBlockMappingToNameNode(filePath, blockData)

}

func (client *ClientData) ReadFile(conn *grpc.ClientConn, source string, fileName string) {

	filePath := filepath.Join(source, fileName)
	nameNodeStub := client.GetNameNodeStub()
	fileData := &namenodeService.FileData{FileName: filePath}
	dataNodes, err := nameNodeStub.GetDataNodesForFile(context.Background(), fileData)
	utils.ErrorHandler(err)
	rand.Seed(10)
	dataNodesBlocks := dataNodes.BlockDataNodes
	for _, blockDataNode := range dataNodesBlocks {
		blockID := blockDataNode.BlockID
		expectedChecksum := blockDataNode.Checksum
		dataNodeIDs := blockDataNode.DataNodeIDs
		startIdx := rand.Intn(len(dataNodeIDs))
		blockRead := false
		for i := 0; i < len(dataNodeIDs); i++ {
			dataNode := dataNodeIDs[(startIdx+i)%len(dataNodeIDs)]
			dataNodeClient := GetDataNodeStub(dataNode.DatanodePort)
			blockRequest := &datanodeService.BlockRequest{BlockID: blockID}
			blockResponse, err := dataNodeClient.ReadBytesFromDataNode(context.Background(), blockRequest)
			if err != nil {
				log.Printf("failed reading block %s from datanode %s: %v", blockID, dataNode.DatanodeID, err)
				continue
			}
			if expectedChecksum != "" {
				actualChecksum := sha256.Sum256(blockResponse.FileContent)
				actualChecksumHex := hex.EncodeToString(actualChecksum[:])
				if actualChecksumHex != expectedChecksum {
					log.Printf("checksum mismatch for block %s from datanode %s", blockID, dataNode.DatanodeID)
					continue
				}
			}
			fmt.Print(string(blockResponse.FileContent))
			blockRead = true
			break
		}
		if !blockRead {
			utils.ErrorHandler(fmt.Errorf("failed to read valid replica for block %s", blockID))
		}
	}

}
