package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkFailure is a startup check that must stop the daemon. Fix tells the
// operator exactly what to change.
type checkFailure struct {
	ID, What, Fix string
}

func (c *checkFailure) Error() string {
	return fmt.Sprintf("[%s] %s\n  fix: %s", c.ID, c.What, c.Fix)
}

// cometConfig and appConfig hold only the keys the checks read.
type cometConfig struct {
	TxIndex struct {
		Indexer string `toml:"indexer"`
	} `toml:"tx_index"`
	Storage struct {
		DiscardABCIResponses bool `toml:"discard_abci_responses"`
		Pruning              struct {
			DataCompanion struct {
				Enabled bool `toml:"enabled"`
			} `toml:"data_companion"`
		} `toml:"pruning"`
	} `toml:"storage"`
	GRPC struct {
		Privileged struct {
			Laddr          string `toml:"laddr"`
			PruningService struct {
				Enabled bool `toml:"enabled"`
			} `toml:"pruning_service"`
		} `toml:"privileged"`
	} `toml:"grpc"`
}

type appConfig struct {
	MinRetainBlocks int64 `toml:"min-retain-blocks"`
}

// preflight runs the startup checks. A returned error stops the daemon.
func preflight(ctx context.Context, cfg *Config, n Node, log *slog.Logger) error {
	// C1/C2: privileged gRPC reachable and PruningService enabled.
	if _, _, err := n.GetRetain(ctx, RetainBlock); err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
			return &checkFailure{"C2", "PruningService is not enabled",
				`config.toml: [grpc.privileged.pruning_service] enabled = true, then restart the node`}
		}
		return &checkFailure{"C1", "privileged gRPC is not reachable at " + cfg.GRPCAddr + ": " + err.Error(),
			`config.toml: [grpc.privileged] laddr = "tcp://127.0.0.1:26659" (bind to 127.0.0.1 only), then restart the node`}
	}

	if cfg.Home == "" {
		log.Warn("check_skipped", "checks", "C3,C4,C7,C8", "reason", "--home not set")
	} else if err := checkConfigFiles(cfg.Home, log); err != nil {
		return err
	}

	// C5: Prometheus is informational only.
	if _, ok := n.Metric(ctx, "application_block_retain_height"); !ok {
		log.Warn("check_warn", "id", "C5", "msg", "Prometheus metrics not reachable; logs will lack tx_base/block_base")
	}

	// C6: an already-large indexer retain height cannot be lowered, and the
	// node will try to prune everything below it in one go.
	latest, _, _, err := n.Status(ctx)
	if err != nil {
		return err
	}
	// Use the larger of the two: the node prunes each index up to its own
	// retain height, so one large value is enough to cause a huge prune.
	cur, ok := int64(0), false
	for _, r := range []Retain{RetainTxIndexer, RetainBlockIndexer} {
		h, set, err := n.GetRetain(ctx, r)
		if err != nil {
			return err
		}
		if set {
			cur, ok = max(cur, h), true
		}
	}
	if ok {
		low, err := lowestIndexed(ctx, n, latest)
		if err != nil {
			return err
		}
		if cur-low > cfg.MaxStep {
			return &checkFailure{"C6",
				fmt.Sprintf("indexer retain height %d is %d blocks above the lowest indexed height %d (max-step %d); the node would prune all of it at once",
					cur, cur-low, low, cfg.MaxStep),
				"stop the node, delete the indexer retain-height keys from data/tx_index.db, start the node, then run the pacer"}
		}
	}
	return nil
}

func checkConfigFiles(home string, log *slog.Logger) error {
	var cc cometConfig
	if _, err := toml.DecodeFile(filepath.Join(home, "config", "config.toml"), &cc); err != nil {
		return &checkFailure{"C3", "cannot read config.toml: " + err.Error(), "check --home"}
	}
	if !cc.Storage.Pruning.DataCompanion.Enabled {
		return &checkFailure{"C3", "data companion pruning is disabled; the node never prunes its indexes",
			"config.toml: [storage.pruning.data_companion] enabled = true, then restart the node"}
	}
	if cc.TxIndex.Indexer != "kv" {
		return &checkFailure{"C4", fmt.Sprintf("tx indexer is %q; only \"kv\" can be pruned", cc.TxIndex.Indexer),
			`nothing to do unless config.toml has [tx_index] indexer = "kv"`}
	}
	if l := cc.GRPC.Privileged.Laddr; l != "" && !strings.Contains(l, "127.0.0.1") && !strings.Contains(l, "localhost") {
		log.Warn("check_warn", "id", "C1", "msg", "privileged gRPC listens on "+l+"; anyone who can reach it can prune this node")
	}
	if cc.Storage.DiscardABCIResponses {
		log.Info("check_info", "id", "C8", "msg", "discard_abci_responses = true; block results are not stored, nothing to prune there")
	}

	var ac appConfig
	if _, err := toml.DecodeFile(filepath.Join(home, "config", "app.toml"), &ac); err != nil {
		// C7 is informational; an app.toml it cannot read must not stop the daemon.
		log.Warn("check_skipped", "id", "C7", "reason", "cannot read app.toml: "+err.Error())
		return nil
	}
	if ac.MinRetainBlocks == 0 {
		log.Warn("check_warn", "id", "C7",
			"msg", "app.toml min-retain-blocks = 0: the app never reports a retain height, so block pruning stays off; index pruning still works")
	}
	return nil
}
