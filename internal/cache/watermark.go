package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const watermarkFileName = ".stravin-watermarks.json"

type watermarkDiskPayload struct {
	Shards map[string]string `json:"shards"`
}

func loadWatermarks(repoPath string) (map[int]string, error) {
	path := filepath.Join(repoPath, watermarkFileName)
	data, err := os.ReadFile(path) //nolint:gosec // path is derived from validated RepoKey under cache root
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[int]string), nil
		}
		return nil, fmt.Errorf("read watermarks: %w", err)
	}

	var payload watermarkDiskPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode watermarks: %w", err)
	}

	out := make(map[int]string, len(payload.Shards))
	for k, v := range payload.Shards {
		shard, err := strconv.Atoi(k)
		if err != nil || shard < 0 || v == "" {
			continue
		}
		out[shard] = v
	}
	return out, nil
}

func saveWatermarks(repoPath string, shards map[int]string) error {
	payload := watermarkDiskPayload{
		Shards: make(map[string]string, len(shards)),
	}
	for shard, ulid := range shards {
		if ulid != "" {
			payload.Shards[strconv.Itoa(shard)] = ulid
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode watermarks: %w", err)
	}
	return writeFileAtomic(repoPath, watermarkFileName, data)
}
