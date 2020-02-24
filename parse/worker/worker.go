package worker

import (
	"fmt"

	"github.com/angelorc/desmos-parser/db"
	"github.com/angelorc/desmos-parser/parse/client"
	"github.com/angelorc/desmos-parser/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog/log"
	tmctypes "github.com/tendermint/tendermint/rpc/core/types"
)

var (
	blockHandlers []BlockHandler
	txHandlers    []TxHandler
	msgHandlers   []MsgHandler
)

// BlockHandler represents a function that allows to handle a single block.
// For convenience of use, all the transactions present inside the given block
// and the currently used database will be passed as well.
type BlockHandler func(block *tmctypes.ResultBlock, txs []types.Tx, db db.Database) error

// RegisterBlockHandler allows to register a new BlockHandler to be called when a new block is parsed.
// All the registered handlers will be called in order as they are registered (First-In-First-Served).
// Later handlers will not execute if a previous handler returns an error.
func RegisterBlockHandler(handler BlockHandler) {
	blockHandlers = append(blockHandlers, handler)
}

// TxHandler represents a function that allows to handle a single transaction.
// For convenience of use, the currently used database will be passed as well.
type TxHandler func(tx types.Tx, db db.Database) error

// RegisterTxHandler allows to register a new TxHandler to be called when a new transaction is parsed.
// All the registered handlers will be called in order as they are registered (First-In-First-Served).
// Later handlers will not execute if a previous handler returns an error.
func RegisterTxHandler(handler TxHandler) {
	txHandlers = append(txHandlers, handler)
}

// MsgHandler represents a function that allows to handle a single transaction message.
// In order to be able to get the logs of that message, or other useful information, the transaction
// that contains it as well as the index of such message inside the transaction itself will be passed too.
// For convenience of use, the currently used database will be passed too.
type MsgHandler func(tx types.Tx, index int, msg sdk.Msg, db db.Database) error

// RegisterMsgHandler allows to register a new MsgHandler to be called when a new message is parsed.
// All the registered handlers will be called in order as they are registered (First-In-First-Served).
// Later handlers will not execute if a previous handler returns an error.
func RegisterMsgHandler(handler MsgHandler) {
	msgHandlers = append(msgHandlers, handler)
}

// Worker defines a job consumer that is responsible for getting and
// aggregating block and associated data and exporting it to a database.
type Worker struct {
	cp    client.ClientProxy
	queue types.Queue
	db    db.Database
}

// NewWorker allows to create a new Worker implementation.
func NewWorker(cp client.ClientProxy, q types.Queue, db db.Database) Worker {
	return Worker{cp, q, db}
}

// Start starts a worker by listening for new jobs (block heights) from the
// given worker queue. Any failed job is logged and re-enqueued.
func (w Worker) Start() {
	for i := range w.queue {
		log.Info().Int64("height", i).Msg("processing block")

		if err := w.process(i); err != nil {
			// re-enqueue any failed job
			// TODO: Implement exponential backoff or max retries for a block height.
			go func() {
				log.Info().Int64("height", i).Msg("re-enqueueing failed block")
				w.queue <- i
			}()
		}
	}
}

// process defines the job consumer workflow. It will fetch a block for a given
// height and associated metadata and export it to a database. It returns an
// error if any export process fails.
func (w Worker) process(height int64) error {
	if exists := w.db.HasBlock(height); exists {
		log.Debug().Int64("height", height).Msg("skipping already exported block with mongodb")
		return nil
	}

	if height == 1 {
		log.Info().Msg("Parse genesis")

		/*if err := w.db.CreateIndexes(); err != nil {
			log.Info().Err(err).Int64("height", height).Msg("error creating index")
		}*/

		/*genesis, err := w.cp.Genesis()
		if err != nil {
			log.Info().Err(err).Int64("height", height).Msg("failed to get genesis")
		}

		return w.db.ExportGenesis(genesis)*/
	}

	block, err := w.cp.Block(height)
	if err != nil {
		log.Info().Err(err).Int64("height", height).Msg("failed to get block")
		return err
	}

	txs, err := w.cp.Txs(block)
	if err != nil {
		log.Info().Err(err).Int64("height", height).Msg("failed to get transactions for block")
		return err
	}

	// Convert the transaction to a more easy-to-handle type
	var txData = make([]types.Tx, len(txs))
	for index, tx := range txs {
		convTx, err := types.NewTx(tx)
		if err != nil {
			return fmt.Errorf("error handleTx")
		}
		txData[index] = *convTx
	}

	/*blockResults, err := w.cp.BlockResults(height)
	if err != nil {
		log.Info().Err(err).Int64("height", height).Msg("failed to get block results")
		return err
	}

	vals, err := w.cp.Validators(block.Block.LastCommit.Height())
	if err != nil {
		log.Info().Err(err).Int64("height", height).Msg("failed to get validators for block")
		return err
	}*/

	// Save the block
	if err := db.SaveBlock(w.db, block, txData); err != nil {
		return err
	}

	// Call the block handlers
	for _, handler := range blockHandlers {
		if err := handler(block, txData, w.db); err != nil {
			return err
		}
	}

	// Handle all the transactions inside the block
	for _, tx := range txData {
		// Save the transaction itself
		if err := db.SaveTx(w.db, tx); err != nil {
			return err
		}

		// Call the tx handlers
		for _, handler := range txHandlers {
			if err := handler(tx, w.db); err != nil {
				return err
			}
		}

		// Handle all the messages contained inside the transaction
		for i, msg := range tx.Messages {
			// Save the message
			if err := w.db.SaveMsg(tx, i, msg); err != nil {
				return err
			}

			// Call the handlers
			for _, handler := range msgHandlers {
				if err := handler(tx, i, msg, w.db); err != nil {
					return err
				}
			}
		}
	}

	return nil
}