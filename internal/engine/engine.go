package engine

import (
	"fmt"

	"github.com/thirawat27/kvi/pkg/config"
	"github.com/thirawat27/kvi/pkg/types"
)

func NewEngine(cfg *config.Config) (types.Engine, error) {
	switch cfg.Mode {
	case types.ModeMemory:
		return NewMemoryEngine(cfg), nil
	case types.ModeDisk:
		return NewDiskEngine(cfg)
	case types.ModeColumnar:
		return NewColumnarEngine(cfg)
	case types.ModeVector:
		return NewVectorEngine(cfg)
	case types.ModeHybrid:
		return NewHybridEngine(cfg)
	default:
		return nil, fmt.Errorf("unknown mode: %s", cfg.Mode)
	}
}
