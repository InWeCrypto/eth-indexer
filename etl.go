package indexer

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/go-xorm/xorm"
	"github.com/inwecrypto/ethdb"
	"github.com/inwecrypto/ethgo/erc20"
	"github.com/inwecrypto/ethgo/erc721"
	"github.com/inwecrypto/ethgo/rpc"
	"github.com/inwecrypto/gomq"
	gomqkafka "github.com/inwecrypto/gomq-kafka"
)

const (
	globalAsset = "0x0000000000000000000000000000000000000000"
)

// ETL .
type ETL struct {
	slf4go.Logger
	conf   *config.Config
	engine *xorm.Engine
	mq     gomq.Producer // mq producer
	topic  string
}

// NewETL .
func NewETL(conf *config.Config) (*ETL, error) {
	username := conf.GetString("indexer.ethdb.username", "xxx")
	password := conf.GetString("indexer.ethdb.password", "xxx")
	port := conf.GetString("indexer.ethdb.port", "6543")
	host := conf.GetString("indexer.ethdb.host", "localhost")
	scheme := conf.GetString("indexer.ethdb.schema", "postgres")

	engine, err := xorm.NewEngine(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v host=%v dbname=%v port=%v sslmode=disable",
			username, password, host, scheme, port,
		),
	)

	if err != nil {
		return nil, err
	}

	mq, err := gomqkafka.NewAliyunProducer(conf)

	if err != nil {
		return nil, err
	}

	return &ETL{
		Logger: slf4go.Get("eth-indexer-etl"),
		conf:   conf,
		engine: engine,
		mq:     mq,
		topic:  conf.GetString("aliyun.kafka.topic", "xxxxx"),
	}, nil
}

// Handle handle eth block
func (etl *ETL) Handle(block *rpc.Block) error {

	ttx := make([]*ethdb.TableTx, 0)

	blocks, err := strconv.ParseUint(strings.TrimPrefix(block.Number, "0x"), 16, 64)

	if err != nil {
		return err
	}

	timestamp, err := strconv.ParseUint(strings.TrimPrefix(block.Timestamp, "0x"), 16, 64)

	if err != nil {
		return err
	}

	for _, tx := range block.Transactions {

		assetID := globalAsset
		value := tx.Value
		to := tx.To

		input := tx.Input

		method := ""
		inputLen := len(input)
		if inputLen >= 10 {
			method = string(input[2:10])
		}

		switch method {

		case erc20.TransferID:
			if inputLen >= 74 {
				to = string(append([]byte{'0', 'x'}, input[34:74]...))
				assetID = tx.To
				value = string(append([]byte{'0', 'x'}, input[74:]...))
			}

		case erc20.ApproveID:
			if inputLen >= 74 {
				to = string(append([]byte{'0', 'x'}, input[34:74]...))
				assetID = erc20.ApproveID + "," + tx.To
				value = string(append([]byte{'0', 'x'}, input[74:]...))
			}

			// land
		case erc721.Method_transferLand:
			if inputLen >= 202 {
				x := string(append([]byte{'0', 'x'}, input[10:74]...))
				y := string(append([]byte{'0', 'x'}, input[74:138]...))

				to = string(append([]byte{'0', 'x'}, input[162:202]...))
				assetID = erc721.Method_transferLand + "," + tx.To
				value = x + "," + y
			}

		default:

		}

		txValue := big.NewInt(0)

		err := txValue.UnmarshalJSON([]byte(tx.Value))

		if err != nil {
			return err
		}

		if assetID == globalAsset || txValue.Uint64() != 0 {
			ttx = append(ttx, &ethdb.TableTx{
				TX:         tx.Hash,
				From:       tx.From,
				To:         tx.To,
				Asset:      assetID,
				Value:      tx.Value,
				Blocks:     blocks,
				Gas:        tx.Gas,
				GasPrice:   tx.GasPrice,
				CreateTime: time.Unix(int64(timestamp), 0),
			})
		}

		if assetID != globalAsset {
			ttx = append(ttx, &ethdb.TableTx{
				TX:         tx.Hash,
				From:       tx.From,
				To:         to,
				Asset:      assetID,
				Value:      value,
				Blocks:     blocks,
				Gas:        tx.Gas,
				GasPrice:   tx.GasPrice,
				CreateTime: time.Unix(int64(timestamp), 0),
			})
		}

		if len(ttx) >= 100 {
			if err := etl.batchInsert(ttx); err != nil {
				return err
			}

			ttx = make([]*ethdb.TableTx, 0)
		}

	}

	if len(ttx) > 0 {
		if err := etl.batchInsert(ttx); err != nil {
			return err
		}
	}

	for _, tx := range block.Transactions {
		if err := etl.mq.Produce(etl.topic, []byte(tx.Hash), tx.Hash); err != nil {
			etl.ErrorF("mq insert err :%s", err)
			return err
		}

		etl.DebugF("tx %s event send", tx.Hash)
	}

	return nil
}

func (etl *ETL) batchInsert(ttx []*ethdb.TableTx) (err error) {
	session := etl.engine.NewSession()

	session.Begin()

	defer func() {
		if err != nil {
			session.Rollback()
		} else {
			session.Commit()
		}
	}()

	_, err = etl.engine.Insert(&ttx)

	return
}
