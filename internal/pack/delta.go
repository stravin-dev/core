package pack

import "fmt"

// ApplyDelta applies a Git packfile delta stream to base and returns the reconstructed target bytes.
// It enforces maxSize to prevent delta-bomb memory exhaustion.
func ApplyDelta(base, delta []byte, maxSize int64) ([]byte, error) {
	pos := 0
	srcSize, n, err := readDeltaVarint(delta[pos:])
	if err != nil {
		return nil, fmt.Errorf("delta source size: %w", err)
	}
	pos += n
	if srcSize != uint64(len(base)) {
		return nil, fmt.Errorf("delta source size %d != base size %d: %w", srcSize, len(base), ErrCorruptEntry)
	}

	targetSize, n, err := readDeltaVarint(delta[pos:])
	if err != nil {
		return nil, fmt.Errorf("delta target size: %w", err)
	}
	pos += n
	if maxSize > 0 && targetSize > uint64(maxSize) {
		return nil, fmt.Errorf("delta target size %d exceeds max %d: %w", targetSize, maxSize, ErrDeltaBomb)
	}

	out := make([]byte, 0, targetSize)
	for pos < len(delta) {
		cmd := delta[pos]
		pos++

		switch {
		case cmd&0x80 != 0:
			var copyOffset, copySize uint64
			if cmd&0x01 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy offset byte 0: %w", ErrCorruptEntry)
				}
				copyOffset |= uint64(delta[pos])
				pos++
			}
			if cmd&0x02 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy offset byte 1: %w", ErrCorruptEntry)
				}
				copyOffset |= uint64(delta[pos]) << 8
				pos++
			}
			if cmd&0x04 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy offset byte 2: %w", ErrCorruptEntry)
				}
				copyOffset |= uint64(delta[pos]) << 16
				pos++
			}
			if cmd&0x08 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy offset byte 3: %w", ErrCorruptEntry)
				}
				copyOffset |= uint64(delta[pos]) << 24
				pos++
			}
			if cmd&0x10 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy size byte 0: %w", ErrCorruptEntry)
				}
				copySize |= uint64(delta[pos])
				pos++
			}
			if cmd&0x20 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy size byte 1: %w", ErrCorruptEntry)
				}
				copySize |= uint64(delta[pos]) << 8
				pos++
			}
			if cmd&0x40 != 0 {
				if pos >= len(delta) {
					return nil, fmt.Errorf("truncated copy size byte 2: %w", ErrCorruptEntry)
				}
				copySize |= uint64(delta[pos]) << 16
				pos++
			}
			if copySize == 0 {
				copySize = 0x10000
			}
			if copyOffset > uint64(len(base)) || copySize > uint64(len(base))-copyOffset {
				return nil, fmt.Errorf("delta copy out of bounds (off=%d size=%d base=%d): %w", copyOffset, copySize, len(base), ErrCorruptEntry)
			}
			if uint64(len(out))+copySize > targetSize {
				return nil, fmt.Errorf("delta copy exceeds target size %d: %w", targetSize, ErrCorruptEntry)
			}
			out = append(out, base[copyOffset:copyOffset+copySize]...)
		case cmd != 0:
			insertLen := int(cmd)
			if pos+insertLen > len(delta) {
				return nil, fmt.Errorf("delta insert truncated: %w", ErrCorruptEntry)
			}
			if uint64(len(out))+uint64(insertLen) > targetSize {
				return nil, fmt.Errorf("delta insert exceeds target size %d: %w", targetSize, ErrCorruptEntry)
			}
			out = append(out, delta[pos:pos+insertLen]...)
			pos += insertLen
		default:
			return nil, fmt.Errorf("reserved delta opcode 0x00: %w", ErrCorruptEntry)
		}
	}

	if uint64(len(out)) != targetSize {
		return nil, fmt.Errorf("delta output size %d != expected %d: %w", len(out), targetSize, ErrCorruptEntry)
	}
	return out, nil
}

func readDeltaVarint(b []byte) (uint64, int, error) {
	var val uint64
	var shift uint
	for i := 0; i < len(b); i++ {
		if shift >= 64 {
			return 0, 0, fmt.Errorf("varint overflow: %w", ErrCorruptEntry)
		}
		c := b[i]
		val |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return val, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, fmt.Errorf("truncated varint: %w", ErrCorruptEntry)
}
