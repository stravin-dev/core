package compaction

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
)

const (
	packTrailerBytes = 20
	bareDirPerm      = 0o700
	bareFilePerm     = 0o600
)

// Tier1Strategy implements deep LSM Tier-1 compaction strictly through the
// sandboxed internal/gitexec subprocess boundary (git repack + multi-pack-index
// + commit-graph) inside an ephemeral temp bare repository.
type Tier1Strategy struct {
	store   PackObjectStore
	gitExec gitexec.GitExecutor
	workDir string
	timeout time.Duration
	limits  pack.Limits
}

// Tier1Config configures Tier1Strategy.
type Tier1Config struct {
	WorkDir string
	Timeout time.Duration
	Limits  pack.Limits
}

// NewTier1Strategy constructs a Tier-1 deep repack strategy.
func NewTier1Strategy(store PackObjectStore, gitExec gitexec.GitExecutor, cfg Tier1Config) (*Tier1Strategy, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil object store", ErrInvalidConfig)
	}
	if gitExec == nil {
		return nil, fmt.Errorf("%w: nil git executor", ErrInvalidConfig)
	}
	return &Tier1Strategy{
		store:   store,
		gitExec: gitExec,
		workDir: cfg.WorkDir,
		timeout: cfg.Timeout,
		limits:  cfg.Limits,
	}, nil
}

// Tier returns Tier1.
func (s *Tier1Strategy) Tier() Tier {
	return Tier1
}

// Compact downloads input packs into an ephemeral bare repo, runs sandboxed
// git repack + multi-pack-index + commit-graph via internal/gitexec, and
// uploads the repacked .pack, .idx, aux/{epoch}.midx, and aux/{epoch}.graph to S3.
func (s *Tier1Strategy) Compact(ctx context.Context, req StrategyRequest) (StrategyResult, error) {
	if len(req.Inputs) == 0 {
		return StrategyResult{}, ErrNothingToCompact
	}

	tmpDir, err := os.MkdirTemp(s.workDir, "stravin-tier1-*")
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: create temp repo: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	if err := initBareRepoLayout(tmpDir); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: init bare repo: %w", err)
	}

	packDir := filepath.Join(tmpDir, "objects", "pack")
	var discoveredCommits []pack.OID

	for _, in := range req.Inputs {
		if err := ctx.Err(); err != nil {
			return StrategyResult{}, err
		}
		packBytes, err := s.store.GetPack(ctx, req.Repo, in.PackID)
		if err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier1: get pack %s: %w", in.PackID, err)
		}
		if len(packBytes) < 12+packTrailerBytes {
			return StrategyResult{}, fmt.Errorf("compaction tier1: pack %s too short (%d bytes)", in.PackID, len(packBytes))
		}
		idxBytes, err := s.store.GetIndex(ctx, req.Repo, in.PackID)
		if err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier1: get idx %s: %w", in.PackID, err)
		}

		// Collect commit OIDs if caller did not supply refs so commit-graph --reachable has tips.
		if len(req.Refs) == 0 {
			if parsed, parseErr := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{Limits: s.limits}); parseErr == nil {
				for oid, obj := range parsed.Objects {
					if obj.Type == pack.ObjCommit {
						discoveredCommits = append(discoveredCommits, oid)
					}
				}
			}
		}

		packHex := hex.EncodeToString(packBytes[len(packBytes)-packTrailerBytes:])
		packPath := filepath.Join(packDir, "pack-"+packHex+".pack")
		idxPath := filepath.Join(packDir, "pack-"+packHex+".idx")
		if err := os.WriteFile(packPath, packBytes, bareFilePerm); err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier1: stage pack %s: %w", in.PackID, err)
		}
		if err := os.WriteFile(idxPath, idxBytes, bareFilePerm); err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier1: stage idx %s: %w", in.PackID, err)
		}
	}

	if err := writePackedRefs(tmpDir, req.Refs, discoveredCommits); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: write packed-refs: %w", err)
	}

	repackReq := gitexec.RepackRequest{
		Repo:             req.Repo,
		RepoDir:          tmpDir,
		Repack:           true,
		All:              true,
		DeleteRedundant:  true,
		Quiet:            true,
		WriteMIDX:        true,
		WriteCommitGraph: true,
		Timeout:          s.timeout,
	}
	if err := s.gitExec.ExecRepack(ctx, repackReq); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: gitexec repack: %w", err)
	}

	packBytes, idxBytes, err := readRepackedArtifact(packDir)
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: read repacked artifact: %w", err)
	}

	packSHA256 := sha256Hex(packBytes)
	if err := s.store.PutPack(ctx, req.Repo, packSHA256, packBytes, idxBytes); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: upload repacked pack %s: %w", packSHA256, err)
	}

	epoch := req.Epoch
	if epoch == "" {
		epoch = fmt.Sprintf("tier1-%d", time.Now().UTC().UnixNano())
	}

	var auxMIDXKey string
	midxPath := filepath.Join(packDir, "multi-pack-index")
	//nolint:gosec // midxPath is constructed strictly inside our ephemeral MkdirTemp directory
	midxBytes, err := os.ReadFile(midxPath)
	if err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: read multi-pack-index: %w", err)
	}
	if err := s.store.PutAuxMIDX(ctx, req.Repo, packSHA256, epoch, midxBytes); err != nil {
		return StrategyResult{}, fmt.Errorf("compaction tier1: upload midx: %w", err)
	}
	auxMIDXKey, _ = s3.AuxMIDXKey(req.Repo, packSHA256, epoch)

	var auxGraphKey string
	graphPath := filepath.Join(tmpDir, "objects", "info", "commit-graph")
	//nolint:gosec // graphPath is constructed strictly inside our ephemeral MkdirTemp directory
	graphBytes, err := os.ReadFile(graphPath)
	if err == nil && len(graphBytes) > 0 {
		if err := s.store.PutAuxGraph(ctx, req.Repo, packSHA256, epoch, graphBytes); err != nil {
			return StrategyResult{}, fmt.Errorf("compaction tier1: upload commit-graph: %w", err)
		}
		auxGraphKey, _ = s3.AuxGraphKey(req.Repo, packSHA256, epoch)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return StrategyResult{}, fmt.Errorf("compaction tier1: read commit-graph: %w", err)
	}

	return StrategyResult{
		OutputPackID: packSHA256,
		OutputSize:   int64(len(packBytes)),
		AuxMIDXKey:   auxMIDXKey,
		AuxGraphKey:  auxGraphKey,
	}, nil
}

func initBareRepoLayout(dir string) error {
	for _, sub := range []string{
		filepath.Join(dir, "objects", "pack"),
		filepath.Join(dir, "objects", "info"),
		filepath.Join(dir, "refs", "heads"),
		filepath.Join(dir, "refs", "tags"),
	} {
		if err := os.MkdirAll(sub, bareDirPerm); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), bareFilePerm); err != nil {
		return err
	}
	cfg := "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n"
	return os.WriteFile(filepath.Join(dir, "config"), []byte(cfg), bareFilePerm)
}

func writePackedRefs(dir string, refs []ddb.RefState, fallbackCommits []pack.OID) error {
	var b strings.Builder
	b.WriteString("# pack-refs with: peeled fully-peeled sorted\n")
	if len(refs) > 0 {
		sorted := make([]ddb.RefState, len(refs))
		copy(sorted, refs)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].RefName < sorted[j].RefName
		})
		for _, r := range sorted {
			if r.OID == "" || r.OID == ddb.ZeroOID || r.RefName == "" {
				continue
			}
			b.WriteString(r.OID)
			b.WriteByte(' ')
			b.WriteString(r.RefName)
			b.WriteByte('\n')
		}
	} else if len(fallbackCommits) > 0 {
		sort.Slice(fallbackCommits, func(i, j int) bool {
			return bytes.Compare(fallbackCommits[i][:], fallbackCommits[j][:]) < 0
		})
		for i, oid := range fallbackCommits {
			fmt.Fprintf(&b, "%s refs/heads/stravin-pack-%d\n", oid.String(), i)
		}
	}
	return os.WriteFile(filepath.Join(dir, "packed-refs"), []byte(b.String()), bareFilePerm)
}

func readRepackedArtifact(packDir string) ([]byte, []byte, error) {
	entries, err := os.ReadDir(packDir)
	if err != nil {
		return nil, nil, err
	}
	var packFile string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".pack") {
			packFile = entry.Name()
			break
		}
	}
	if packFile == "" {
		return nil, nil, errors.New("no repacked .pack file produced")
	}
	idxFile := strings.TrimSuffix(packFile, ".pack") + ".idx"

	//nolint:gosec // packDir is our ephemeral MkdirTemp objects/pack directory
	packBytes, err := os.ReadFile(filepath.Join(packDir, packFile))
	if err != nil {
		return nil, nil, err
	}
	//nolint:gosec // packDir is our ephemeral MkdirTemp objects/pack directory
	idxBytes, err := os.ReadFile(filepath.Join(packDir, idxFile))
	if err != nil {
		return nil, nil, err
	}
	return packBytes, idxBytes, nil
}
