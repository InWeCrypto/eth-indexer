package indexer

import (
	"encoding/binary"
	"strconv"
	"strings"
	"time"

	"github.com/dynamicgo/slf4go"
	"github.com/inwecrypto/ethgo/rpc"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/dynamicgo/config"
)

var key = []byte("key")

// Monitor .
type Monitor struct {
	slf4go.Logger
	client       *rpc.Client
	etl          *ETL
	pullDuration time.Duration
	db           *leveldb.DB
	blocks       chan *rpc.Block
}

// NewMonitor .
func NewMonitor(conf *config.Config) (*Monitor, error) {
	client := rpc.NewClient(conf.GetString("indexer.geth", "http://localhost:8545"))

	etl, err := NewETL(conf)

	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(conf.GetString("indexer.localdb", "./cursor"), nil)

	if err != nil {
		return nil, err
	}

	return &Monitor{
		Logger:       slf4go.Get("eth-monitor"),
		client:       client,
		etl:          etl,
		pullDuration: time.Second * conf.GetDuration("indexer.pull", 4),
		db:           db,
		blocks:       make(chan *rpc.Block, conf.GetInt64("indexer.cached", 100)),
	}, nil
}

// Run start monitor
func (monitor *Monitor) Run() {
	ticker := time.NewTicker(monitor.pullDuration)

	go monitor.doETL()

	for _ = range ticker.C {
		monitor.DebugF("fetch geth last block number ...")
		blocks, err := monitor.client.BlockNumber()
		monitor.DebugF("fetch geth last block number -- success, %d", blocks)
		if err != nil {
			monitor.ErrorF("fetch geth blocks err, %s", err)
		}

		for current := monitor.getCursor(); current < blocks; current++ {
			if err := monitor.fetchBlock(current); err != nil {
				break
			}
		}
	}
}

func (monitor *Monitor) doETL() {

	for block := range monitor.blocks {

		blockNumber, err := strconv.ParseUint(strings.TrimPrefix(block.Number, "0x"), 16, 64)

		if err != nil {
			time.Sleep(monitor.pullDuration)
			continue
		}

		monitor.DebugF("etl handle block(%d) ...", blockNumber)

		if err := monitor.etl.Handle(block); err != nil {
			monitor.ErrorF("etl handle geth block(%d) err, %s", blockNumber, err)
			time.Sleep(monitor.pullDuration)
			continue
		}

		monitor.DebugF("etl handle block(%d) -- success", blockNumber)

		if err := monitor.setCursor(blockNumber + 1); err != nil {
			monitor.ErrorF("monitor set cursor(%d) err, %s", blockNumber, err)
			time.Sleep(monitor.pullDuration)
			continue
		}
	}
}

func (monitor *Monitor) fetchBlock(blockNumber uint64) error {

	// blockNumber := monitor.getCursor()

	monitor.DebugF("fetch block(%d) ...", blockNumber)

	block, err := monitor.client.GetBlockByNumber(blockNumber)

	if err != nil {
		monitor.ErrorF("fetch geth block(%d) err, %s", blockNumber, err)
		return err
	}

	monitor.DebugF("fetch block(%d) -- success", blockNumber)

	monitor.blocks <- block

	return nil
}

func (monitor *Monitor) getCursor() uint64 {
	buff, err := monitor.db.Get(key, nil)

	if err != nil {
		monitor.ErrorF("get Monitor local cursor error :%s", err)
		return 0
	}

	if buff == nil {
		monitor.ErrorF("get Monitor local cursor error : cursor not exists")
		return 0
	}

	return binary.BigEndian.Uint64(buff)
}

func (monitor *Monitor) setCursor(cursor uint64) error {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, cursor)

	return monitor.db.Put(key, buff, nil)
}
