package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// common definition for snapshot hooks

// an ideal structure might look like the following:
// [fsdir]/serve/[mirror_name] -> a soft link to ../snapshot/[mirror_name]/@[unix_timestamp]
// [fsdir]/working/[mirror_name] -> a rw subvolume created from ../snapshot/[mirror_name]/@[unix_timestamp]
// [fsdir]/snapshot/[mirror_name]/@[unix_timestamp] -> the latest ro snapshot / subvolume
type providerSnapshotConfig struct {
	// global mirror serve dir
	mirrorDir string
	// a soft link to the latest snapshot (ideally ro)
	mirrorServeDir string
	// created subvolume (ideally rw) for syncing, and should only exist when syncing
	// case: succeeded => create a new ro snapshot (@[timestamp]), update the soft link and delete the previous one
	// case: failed => ~~delete the rw subvolume~~ keep it to resume progress
	mirrorWorkingDir string
	// store all (ideally ro) snapshots
	mirrorSnapshotDir string

	uid int
	gid int
}

func (c *providerSnapshotConfig) NewSnapshotName() string {
	return fmt.Sprintf("@%d", time.Now().Unix())
}

// create dir and chown to defined user/group
func (c *providerSnapshotConfig) tryCreateAndChownDir(path string) {
	fullPath, err := filepath.Abs(path)
	if err != nil {
		logger.Errorf("failed to get abs path %s: %s", path, err.Error())
	}
	if _, err = os.Stat(fullPath); os.IsNotExist(err) {
		err := os.MkdirAll(fullPath, 0755)
		if err != nil {
			logger.Errorf("failed to create dir %s: %s", fullPath, err.Error())
		}
		if err := os.Chown(fullPath, c.uid, c.gid); err != nil {
			logger.Warningf("failed to chown dir %s: %s", fullPath, err.Error())
		}
	}
}
